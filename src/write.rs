use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, slice};

use compio_buf::{BufResult, IntoInner, IoBuf, IoBufMut};
use futures_util::task::AtomicWaker;

type WriteFut<'a, Io: compio_io::AsyncWrite + 'a, Buf: IoBufMut> =
    impl Future<Output = (Io, BufResult<usize, Buf>)> + 'a;

type FlushFut<'a, Io: compio_io::AsyncWrite + 'a, Buf: IoBufMut> =
    impl Future<Output = (Io, io::Result<()>)> + 'a;

type CloseFut<'a, Io: compio_io::AsyncWrite + 'a, Buf: IoBufMut> =
    impl Future<Output = (Io, io::Result<()>)> + 'a;

enum FutState<'a, Io: compio_io::AsyncWrite + 'a, Buf: IoBufMut> {
    Idle,
    Write(ManuallyDrop<WriteFut<'a, Io, Buf>>),
    Flush(ManuallyDrop<FlushFut<'a, Io, Buf>>),
    Close(ManuallyDrop<CloseFut<'a, Io, Buf>>),
}

pub struct CompatWrite<'a, Io: compio_io::AsyncWrite + 'a, Buf: IoBufMut> {
    io: Option<Io>,
    fut: FutState<'a, Io, Buf>,
    write_waker: AtomicWaker,
    flush_waker: AtomicWaker,
    close_waker: AtomicWaker,
    buf: Option<Buf>,
}

impl<'a, Io: compio_io::AsyncWrite + 'a, Buf: IoBufMut> Drop for CompatWrite<'a, Io, Buf> {
    fn drop(&mut self) {
        match &mut self.fut {
            FutState::Idle => {}
            FutState::Write(fut) => {
                // safety: we won't use again
                unsafe {
                    ManuallyDrop::drop(fut);
                }
            }
            FutState::Flush(fut) => {
                // safety: we won't use again
                unsafe {
                    ManuallyDrop::drop(fut);
                }
            }
            FutState::Close(fut) => {
                // safety: we won't use again
                unsafe {
                    ManuallyDrop::drop(fut);
                }
            }
        }
    }
}

impl<'a, Io: compio_io::AsyncWrite + 'a, Buf: IoBufMut> CompatWrite<'a, Io, Buf> {
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

impl<'a, Io: compio_io::AsyncWrite + 'a, Buf: IoBufMut> futures_util::AsyncWrite
    for CompatWrite<'a, Io, Buf>
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        // safety: we don't move self
        let this = unsafe { self.get_unchecked_mut() };
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

                    this.fut = FutState::Write(ManuallyDrop::new(async move {
                        let BufResult(res, buf) = io.write(buf).await;

                        (io, BufResult(res, buf.into_inner()))
                    }));
                }

                FutState::Write(fut) => {
                    // safety: we don't move fut until it is completed
                    return match unsafe { Pin::new_unchecked(&mut **fut) }.poll(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready((io, BufResult(res, buf))) => {
                            this.io = Some(io);
                            this.buf = Some(buf);

                            // safety: we won't use again
                            unsafe {
                                ManuallyDrop::drop(fut);
                            }
                            this.fut = FutState::Idle;

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
        // safety: we don't move self
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match &mut this.fut {
                FutState::Idle => {
                    let mut io = this.io.take().unwrap();

                    this.fut = FutState::Flush(ManuallyDrop::new(async move {
                        let res = io.flush().await;

                        (io, res)
                    }));
                }

                FutState::Write(_) => {
                    this.flush_waker.register(cx.waker());

                    return Poll::Pending;
                }

                FutState::Flush(fut) => {
                    // safety: we don't move fut until it is completed
                    return match unsafe { Pin::new_unchecked(&mut **fut) }.poll(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready((io, res)) => {
                            this.io = Some(io);

                            // safety: we won't use again
                            unsafe {
                                ManuallyDrop::drop(fut);
                            }
                            this.fut = FutState::Idle;

                            // wait other pending tasks
                            this.write_waker.wake();
                            this.close_waker.wake();

                            Poll::Ready(res)
                        }
                    };
                }

                FutState::Close(_) => {
                    this.flush_waker.register(cx.waker());

                    return Poll::Pending;
                }
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // safety: we don't move self
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match &mut this.fut {
                FutState::Idle => {
                    let mut io = this.io.take().unwrap();

                    this.fut = FutState::Close(ManuallyDrop::new(async move {
                        let res = io.shutdown().await;

                        (io, res)
                    }));
                }

                FutState::Write(_) => {
                    this.close_waker.register(cx.waker());

                    return Poll::Pending;
                }

                FutState::Close(fut) => {
                    // safety: we don't move fut until it is completed
                    return match unsafe { Pin::new_unchecked(&mut **fut) }.poll(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready((io, res)) => {
                            this.io = Some(io);

                            // safety: we won't use again
                            unsafe {
                                ManuallyDrop::drop(fut);
                            }
                            this.fut = FutState::Idle;

                            // wait other pending tasks
                            this.write_waker.wake();
                            this.flush_waker.wake();

                            Poll::Ready(res)
                        }
                    };
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
    use std::pin::pin;

    use compio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
    use compio::runtime;
    use compio::runtime::Runtime;
    use compio_io::AsyncRead;
    use futures_util::AsyncWriteExt;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_tcp_write() {
        Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let task = runtime::spawn(async move {
                let tcp_stream = listener.accept().await.unwrap().0;

                CompatWrite::new(tcp_stream, vec![0; 100])
            });

            let mut tcp_stream = TcpStream::connect(addr).await.unwrap();

            let compat_write = task.await;
            let mut compat_write = pin!(compat_write);
            compat_write.write_all(b"test").await.unwrap();
            compat_write.flush().await.unwrap();
            compat_write.close().await.unwrap();

            let buf = vec![0; 4];
            let (n, buf) = tcp_stream.read(buf).await.unwrap();

            assert_eq!(&buf[..n], b"test");
        });
    }

    #[test]
    fn test_uds_write() {
        Runtime::new().unwrap().block_on(async {
            let dir = TempDir::new_in(env::temp_dir()).unwrap();
            let path = dir.path().join("test.sock");
            let unix_listener = UnixListener::bind(&path).unwrap();

            let task = runtime::spawn(async move { unix_listener.accept().await.unwrap().0 });

            let mut unix_stream = UnixStream::connect(path).unwrap();
            let unix_stream2 = task.await;
            let compat_write = CompatWrite::new(unix_stream2, vec![0; 100]);
            let mut compat_write = pin!(compat_write);
            compat_write.write_all(b"test").await.unwrap();
            compat_write.flush().await.unwrap();
            compat_write.close().await.unwrap();

            let buf = vec![0; 4];
            let (n, buf) = unix_stream.read(buf).await.unwrap();

            assert_eq!(&buf[..n], b"test");
        });
    }
}
