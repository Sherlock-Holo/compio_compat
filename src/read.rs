use std::future::Future;
use std::io;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use compio_buf::{BufResult, IoBufMut};
use futures_util::AsyncBufRead;

type Fut<'a, Io: compio_io::AsyncRead + 'a, Buf: IoBufMut> =
    impl Future<Output = (Io, BufResult<usize, Buf>)> + 'a;

pub struct CompatRead<'a, Io: compio_io::AsyncRead + 'a, Buf: IoBufMut> {
    io: Option<Io>,
    fut: Option<ManuallyDrop<Fut<'a, Io, Buf>>>,
    buf: Option<Buf>,
    data_size: usize,
}

impl<'a, Io: compio_io::AsyncRead + 'a, Buf: IoBufMut> CompatRead<'a, Io, Buf> {
    pub fn new(io: Io, buf: Buf) -> Self {
        Self {
            io: Some(io),
            fut: None,
            buf: Some(buf),
            data_size: 0,
        }
    }
}

impl<'a, Io: compio_io::AsyncRead + Unpin + 'a, Buf: IoBufMut + Unpin> Unpin
    for CompatRead<'a, Io, Buf>
{
}

impl<'a, Io: compio_io::AsyncRead + 'a, Buf: IoBufMut> AsyncBufRead for CompatRead<'a, Io, Buf> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        // safety: we don't move self
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            match this.fut.as_mut() {
                // safety: we won't move it unless fut is completed
                Some(fut) => match unsafe { Pin::new_unchecked(&mut **fut) }.poll(cx) {
                    Poll::Pending => return Poll::Pending,

                    Poll::Ready((io, BufResult(res, buf))) => {
                        this.io = Some(io);
                        this.buf = Some(buf);

                        // safety: we won't use it again
                        unsafe {
                            ManuallyDrop::drop(fut);
                        }
                        this.fut.take();

                        let n = res?;
                        this.data_size = n;
                    }
                },

                None => {
                    if this.data_size > 0 {
                        let buf = this.buf.as_ref().unwrap();
                        return Poll::Ready(Ok(&buf.as_slice()[..this.data_size]));
                    }

                    let buf = this.buf.take().unwrap();
                    let mut io = this.io.take().unwrap();

                    this.fut = Some(ManuallyDrop::new(async move {
                        let buf_res = io.read(buf).await;

                        (io, buf_res)
                    }));
                }
            }
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        // safety: we don't move self
        unsafe { self.get_unchecked_mut() }.data_size -= amt;
    }
}

impl<'a, Io: compio_io::AsyncRead + 'a, Buf: IoBufMut> futures_util::AsyncRead
    for CompatRead<'a, Io, Buf>
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let data = ready!(self.as_mut().poll_fill_buf(cx))?;
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let size = data.len().min(buf.len());
        buf[..size].copy_from_slice(&data[..size]);
        self.as_mut().consume(size);

        Poll::Ready(Ok(size))
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use compio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
    use compio::runtime;
    use compio::runtime::Runtime;
    use compio_io::AsyncWriteExt;
    use futures_util::AsyncReadExt;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_tcp_read() {
        Runtime::new().unwrap().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            runtime::spawn(async move {
                let mut tcp_stream = listener.accept().await.unwrap().0;
                tcp_stream.write_all(b"test").await.0.unwrap();
            })
            .detach();

            let tcp_stream = TcpStream::connect(addr).await.unwrap();
            let mut compat_read = CompatRead::new(tcp_stream, vec![0; 100]);

            let mut buf = [0; 100];
            let n = compat_read.read(&mut buf).await.unwrap();

            assert_eq!(&buf[..n], b"test");
        });
    }

    #[test]
    fn test_uds_read() {
        Runtime::new().unwrap().block_on(async {
            let dir = TempDir::new_in(env::temp_dir()).unwrap();
            let path = dir.path().join("test.sock");
            let unix_listener = UnixListener::bind(&path).unwrap();

            runtime::spawn(async move {
                let mut unix_stream = unix_listener.accept().await.unwrap().0;
                unix_stream.write_all(b"test").await.0.unwrap();
            })
            .detach();

            let unix_stream = UnixStream::connect(path).unwrap();
            let mut compat_read = CompatRead::new(unix_stream, vec![0; 100]);

            let mut buf = [0; 100];
            let n = compat_read.read(&mut buf).await.unwrap();

            assert_eq!(&buf[..n], b"test");
        });
    }
}
