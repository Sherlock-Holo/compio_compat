use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, slice};

use compio_buf::{BufResult, IntoInner, IoBuf, IoBufMut};
use futures_util::task::AtomicWaker;

type WriteFut<'a, Io: compio_io::AsyncWrite + Unpin + 'a, Buf: IoBufMut + Unpin> =
    impl Future<Output = (Io, BufResult<usize, Buf>)> + 'a + Unpin;

type FlushFut<'a, Io: compio_io::AsyncWrite + Unpin + 'a, Buf: IoBufMut + Unpin> =
    impl Future<Output = (Io, io::Result<()>)> + 'a + Unpin;

type CloseFut<'a, Io: compio_io::AsyncWrite + Unpin + 'a, Buf: IoBufMut + Unpin> =
    impl Future<Output = (Io, io::Result<()>)> + 'a + Unpin;

enum FutState<'a, Io: compio_io::AsyncWrite + Unpin + 'a, Buf: IoBufMut + Unpin> {
    Idle,
    Write(WriteFut<'a, Io, Buf>),
    Flush(FlushFut<'a, Io, Buf>),
    Close(CloseFut<'a, Io, Buf>),
}

pub struct CompatWrite<'a, Io: compio_io::AsyncWrite + Unpin + 'a, Buf: IoBufMut + Unpin> {
    io: Option<Io>,
    fut: FutState<'a, Io, Buf>,
    write_waker: AtomicWaker,
    flush_waker: AtomicWaker,
    close_waker: AtomicWaker,
    buf: Option<Buf>,
}

impl<'a, Io: compio_io::AsyncWrite + Unpin + 'a, Buf: IoBufMut + Unpin> CompatWrite<'a, Io, Buf> {
    pub fn new(io: Io, buf: Buf) -> Self {
        Self {
            io: Some(io),
            fut: FutState::Idle,
            write_waker: Default::default(),
            flush_waker: Default::default(),
            close_waker: Default::default(),
            buf: Some(buf),
        }
    }
}

impl<'a, Io: compio_io::AsyncWrite + Unpin + 'a, Buf: IoBufMut + Unpin> futures_util::AsyncWrite
    for CompatWrite<'a, Io, Buf>
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            match &mut this.fut {
                FutState::Idle => {
                    let mut io = this.io.take().unwrap();
                    let buf = this.buf.take().unwrap();
                    let size = buf.buf_capacity().min(data.len());
                    let mut buf = buf.slice(..size);
                    {
                        // Safety: we don't read it
                        let buf = unsafe {
                            slice::from_raw_parts_mut(buf.as_buf_mut_ptr(), buf.buf_capacity())
                        };
                        buf.copy_from_slice(&data[..size]);
                    }

                    this.fut = FutState::Write(Box::pin(async move {
                        let BufResult(res, buf) = io.write(buf).await;

                        (io, BufResult(res, buf.into_inner()))
                    }));
                }

                FutState::Write(ref mut fut) => {
                    return match Pin::new(fut).poll(cx) {
                        Poll::Pending => Poll::Pending,

                        Poll::Ready((io, BufResult(res, buf))) => {
                            this.io = Some(io);
                            this.buf = Some(buf);

                            // wait other pending tasks
                            this.flush_waker.wake();
                            this.close_waker.wake();

                            Poll::Ready(res)
                        }
                    };
                }

                FutState::Flush(_) => {
                    this.write_waker.register(cx.waker());

                    return Poll::Pending;
                }

                FutState::Close(_) => {
                    this.write_waker.register(cx.waker());

                    return Poll::Pending;
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match &mut this.fut {
                FutState::Idle => {
                    let mut io = this.io.take().unwrap();

                    this.fut = FutState::Flush(Box::pin(async move {
                        let res = io.flush().await;

                        (io, res)
                    }));
                }

                FutState::Write(_) => {
                    this.flush_waker.register(cx.waker());

                    return Poll::Pending;
                }

                FutState::Flush(ref mut fut) => {
                    return match Pin::new(fut).poll(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready((io, res)) => {
                            this.io = Some(io);
                            this.write_waker.wake();
                            this.close_waker.wake();

                            Poll::Ready(res)
                        }
                    }
                }

                FutState::Close(_) => {
                    this.flush_waker.register(cx.waker());

                    return Poll::Pending;
                }
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match &mut this.fut {
                FutState::Idle => {
                    let mut io = this.io.take().unwrap();

                    this.fut = FutState::Close(Box::pin(async move {
                        let res = io.shutdown().await;

                        (io, res)
                    }));
                }

                FutState::Write(_) => {
                    this.close_waker.register(cx.waker());

                    return Poll::Pending;
                }

                FutState::Close(ref mut fut) => {
                    return match Pin::new(fut).poll(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready((io, res)) => {
                            this.io = Some(io);
                            this.write_waker.wake();
                            this.flush_waker.wake();

                            Poll::Ready(res)
                        }
                    }
                }

                FutState::Flush(_) => {
                    this.close_waker.register(cx.waker());

                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use compio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
    use compio::runtime;
    use compio::runtime::Runtime;
    use compio_io::AsyncReadExt;
    use futures_util::AsyncWriteExt;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_tcp_write() {
        Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            runtime::spawn(async move {
                let tcp_stream = listener.accept().await.unwrap().0;
                let mut compat_write = CompatWrite::new(tcp_stream, vec![0; 100]);

                compat_write.write_all(b"test").await.unwrap();
            })
            .detach();

            let mut tcp_stream = TcpStream::connect(addr).await.unwrap();

            let buf = vec![0; 4];
            let (n, buf) = tcp_stream.read_exact(buf).await.unwrap();

            assert_eq!(&buf[..n], b"test");
        });
    }

    #[test]
    fn test_udp_read() {
        Runtime::new().unwrap().block_on(async {
            let dir = TempDir::new_in(env::temp_dir()).unwrap();
            let path = dir.path().join("test.sock");
            let unix_listener = UnixListener::bind(&path).unwrap();

            runtime::spawn(async move {
                let unix_stream = unix_listener.accept().await.unwrap().0;
                let mut compat_write = CompatWrite::new(unix_stream, vec![0; 100]);

                compat_write.write_all(b"test").await.unwrap();
            })
            .detach();

            let mut unix_stream = UnixStream::connect(path).unwrap();

            let buf = vec![0; 4];
            let (n, buf) = unix_stream.read_exact(buf).await.unwrap();

            assert_eq!(&buf[..n], b"test");
        });
    }
}
