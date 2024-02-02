use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, mem, slice};

use compio_buf::{BufResult, IntoInner, IoBuf, IoBufMut};
use futures_util::task::AtomicWaker;
use futures_util::FutureExt;

type WriteFut<'a, Io: compio_io::AsyncWrite + Unpin + 'a, Buf: IoBufMut + Unpin> =
    impl Future<Output = (Io, BufResult<usize, Buf>)> + 'a + Unpin;

type FlushFut<'a, Io: compio_io::AsyncWrite + Unpin + 'a> =
    impl Future<Output = (Io, io::Result<()>)> + 'a + Unpin;

type CloseFut<'a, Io: compio_io::AsyncWrite + Unpin + 'a> =
    impl Future<Output = (Io, io::Result<()>)> + 'a + Unpin;

enum FutState<'a, Io: compio_io::AsyncWrite + Unpin + 'a, Buf: IoBufMut + Unpin> {
    Idle,
    Write(WriteFut<'a, Io, Buf>),
    Flush(FlushFut<'a, Io>),
    Close(CloseFut<'a, Io>),
}

pub struct CompatWrite<'a, Io: compio_io::AsyncWrite + Unpin + 'a, Buf: IoBufMut + Unpin> {
    io: Option<Io>,
    fut: FutState<'a, Io, Buf>,
    write_waker: AtomicWaker,
    flush_waker: AtomicWaker,
    close_waker: AtomicWaker,
    buf: Option<Buf>,
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
            unsafe {
                match mem::replace(&mut this.fut, FutState::Idle) {
                    FutState::Idle => {
                        let mut io = this.io.take().unwrap();
                        let buf = this.buf.take().unwrap();
                        let size = buf.buf_capacity().min(data.len());
                        let mut buf = buf.slice(..size);
                        {
                            // Safety: we don't read it
                            let buf =
                                slice::from_raw_parts_mut(buf.as_buf_mut_ptr(), buf.buf_capacity());
                            buf.copy_from_slice(&data[..size]);
                        }

                        this.fut = FutState::Write(
                            async move {
                                let BufResult(res, buf) = io.write(buf).await;

                                (io, BufResult(res, buf.into_inner()))
                            }
                            .boxed_local(),
                        );
                    }
                    FutState::Write(mut fut) => {
                        return match Pin::new(&mut fut).poll(cx) {
                            Poll::Pending => {
                                this.fut = FutState::Write(fut);

                                Poll::Pending
                            }

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

                    FutState::Flush(fut) => {
                        this.write_waker.register(cx.waker());
                        this.fut = FutState::Flush(fut);

                        return Poll::Pending;
                    }

                    FutState::Close(fut) => {
                        this.write_waker.register(cx.waker());
                        this.fut = FutState::Close(fut);

                        return Poll::Pending;
                    }
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match mem::replace(&mut this.fut, FutState::Idle) {
                FutState::Idle => {
                    let mut io = this.io.take().unwrap();

                    this.fut = FutState::Flush(
                        async move {
                            let res = io.flush().await;

                            (io, res)
                        }
                        .boxed_local(),
                    );
                }

                FutState::Write(fut) => {
                    this.flush_waker.register(cx.waker());
                    this.fut = FutState::Write(fut);

                    return Poll::Pending;
                }

                FutState::Flush(mut fut) => {
                    return match Pin::new(&mut fut).poll(cx) {
                        Poll::Pending => {
                            this.fut = FutState::Flush(fut);

                            Poll::Pending
                        }
                        Poll::Ready((io, res)) => {
                            this.io = Some(io);
                            this.write_waker.wake();
                            this.close_waker.wake();

                            Poll::Ready(res)
                        }
                    }
                }

                FutState::Close(fut) => {
                    this.flush_waker.register(cx.waker());
                    this.fut = FutState::Close(fut);

                    return Poll::Pending;
                }
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            match mem::replace(&mut this.fut, FutState::Idle) {
                FutState::Idle => {
                    let mut io = this.io.take().unwrap();

                    this.fut = FutState::Close(
                        async move {
                            let res = io.shutdown().await;

                            (io, res)
                        }
                        .boxed_local(),
                    );
                }

                FutState::Write(fut) => {
                    this.close_waker.register(cx.waker());
                    this.fut = FutState::Write(fut);

                    return Poll::Pending;
                }

                FutState::Close(mut fut) => {
                    return match Pin::new(&mut fut).poll(cx) {
                        Poll::Pending => {
                            this.fut = FutState::Close(fut);

                            Poll::Pending
                        }
                        Poll::Ready((io, res)) => {
                            this.io = Some(io);
                            this.write_waker.wake();
                            this.flush_waker.wake();

                            Poll::Ready(res)
                        }
                    }
                }

                FutState::Flush(fut) => {
                    this.close_waker.register(cx.waker());
                    this.fut = FutState::Flush(fut);

                    return Poll::Pending;
                }
            }
        }
    }
}
