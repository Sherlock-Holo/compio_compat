use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use compio_buf::{BufResult, IoBufMut};
use futures_core::future::LocalBoxFuture;
use futures_io::AsyncBufRead;

pub struct CompatRead<Io, Buf> {
    io: Option<Io>,
    fut: Option<LocalBoxFuture<'static, (Io, BufResult<usize, Buf>)>>,
    buf: Option<Buf>,
    data_size: usize,
}

impl<Io, Buf> CompatRead<Io, Buf> {
    pub fn new(io: Io, buf: Buf) -> Self {
        Self {
            io: Some(io),
            fut: None,
            buf: Some(buf),
            data_size: 0,
        }
    }
}

impl<Io: compio_io::AsyncRead + Unpin + 'static, Buf: IoBufMut + Unpin> AsyncBufRead
    for CompatRead<Io, Buf>
{
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        loop {
            match this.fut.take() {
                Some(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        this.fut = Some(fut);

                        return Poll::Pending;
                    }

                    Poll::Ready((io, BufResult(res, buf))) => {
                        this.io = Some(io);
                        this.buf = Some(buf);

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

                    this.fut = Some(Box::pin(async move {
                        let buf_res = io.read(buf).await;

                        (io, buf_res)
                    }));
                }
            }
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.get_mut().data_size -= amt;
    }
}

impl<Io: compio_io::AsyncRead + Unpin + 'static, Buf: IoBufMut + Unpin> futures_io::AsyncRead
    for CompatRead<Io, Buf>
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
    use compio::net::{TcpListener, TcpStream};
    use compio::runtime;
    use compio::runtime::Runtime;
    use compio_io::AsyncWriteExt;
    use futures_util::AsyncReadExt;

    use super::*;

    #[test]
    fn test_read() {
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
}
