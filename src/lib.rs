#![feature(type_alias_impl_trait)]

pub use read::CompatRead;
pub use write::CompatWrite;

mod read;
mod write;
