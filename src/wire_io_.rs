//! 面向 [`TrBuffRead`] / [`TrBuffWrite`] 的「读满 / 写全」字节游标。
//!
//! 握手帧与复用帧都按「自描述字段序列」成形，两者都要把底层按需借出的段拼成
//! 定长的字节序列（读侧），或把一段字节分多次写出去（写侧）。这段逻辑在本 crate
//! 内只实现一次：它包含**唯一一处 `unsafe`**（把借出的 `MaybeUninit<u8>` 段搬进
//! 已初始化的 `[u8]`），不适合在两个协议模块里各抄一份。
//!
//! 错误类型保持中立（[`CursorError`]），由各协议模块自行映射：握手侧映射为
//! `WireError`，复用侧映射为 [`MuxError`](crate::connection::MuxError)。
//!
//! 本模块是 crate 内部实现细节，不对外导出。

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef},
    x_deps::abs_cancel,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::x_deps::abs_buff;

/// 字节游标操作失败。
///
/// 只表达「底层怎么失败的」，不含任何协议语义；两个协议模块各自把它映射成
/// 面向使用者的错误类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorError<RE, WE> {
    /// 底层读半边失败。
    Read(RE),

    /// 底层写半边失败。
    Write(WE),

    /// 未读满 / 未写完时对端已关闭。
    PeerClosed,
}

/// 读侧游标：按块读满目标长度。
///
/// 底层缓冲可能一次只交付部分字节，因此这里以**块**为单位重试：每次
/// [`TrBuffRead::read_async`] 借出的段在被回收时会把已消费量记回缓冲，
/// 下一次重试继续往后读，直到满足目标长度或对端关闭。这里**不累计帧长**、
/// 也**不设帧长上限**：帧长本身是无界的（见各协议模块文档）。
pub(crate) struct ReadCursor<'f, R> {
    buff_: &'f mut R,
}

impl<'f, R> ReadCursor<'f, R>
where
    R: TrBuffRead<u8>,
{
    /// 用读半边包出一个游标。
    pub(crate) fn new_(buff: &'f mut R) -> Self {
        ReadCursor { buff_: buff }
    }

    /// 读满一个字节。
    ///
    /// # Errors
    ///
    /// 底层读失败 → [`CursorError::Read`]；读满之前对端关闭 →
    /// [`CursorError::PeerClosed`]。
    pub(crate) async fn read_byte_async_<K>(
        &mut self,
        cancel: K,
    ) -> Result<u8, CursorError<R::Err, ()>>
    where
        K: TrCancellationToken,
    {
        let mut one = [0u8; 1];
        self.read_async_(&mut one, cancel).await?;
        Result::Ok(one[0])
    }

    /// 分多次读满 `out`。
    ///
    /// [`TrBuffRead::read_async`] 借出的段可能比请求的更长，这里只取需要的
    /// 部分，多出的字节留在底层缓冲里。
    ///
    /// # 为什么索取粒度是「至少 1 字节」而不是「正好 `rest`」
    ///
    /// `buffex::ring` 在 `Demand` 的下限大于环容量时返回**终态**的
    /// `Unsatisfiable`（见 [`ReadCursor::read_byte_async_`] 的说明）。若这里按剩余
    /// 长度索要，则「环容量 < 本次要读的长度」会直接判连接失败——而这是**调用方注入
    /// 的环容量**（`ConnRx`、帧暂存环），不该由一次性读取的粒度决定成败。
    ///
    /// 改为每次只要 1 字节起步、按底层实际给出的段长推进后，`min_len == 1` 不超过任何
    /// 合法容量，`Unsatisfiable` 这条路径从构造上消失；语义不变（要么读满 `out`，
    /// 要么在中途遇到读错误 / 对端关闭而失败）。
    ///
    /// # Errors
    ///
    /// 同 [`ReadCursor::read_byte_async_`]。
    pub(crate) async fn read_async_<K>(
        &mut self,
        out: &mut [u8],
        cancel: K,
    ) -> Result<(), CursorError<R::Err, ()>>
    where
        K: TrCancellationToken,
    {
        if out.is_empty() {
            return Result::Ok(());
        }
        let mut offset = 0usize;
        while offset < out.len() {
            let rest = out.len() - offset;
            // 下限只用 1：环容量再小也满足。段可能比 `rest` 更长，下面按 `rest` 截断。
            let demand = Demand::at_least(1usize);
            let got;
            {
                // 段只提供只读视图；消费量由 `move_items_to_buff` 提交，
                // 段回收时游标才会前进（见 abs_buff 的消费语义）。
                let mut read_res = self
                    .buff_
                    .read_async(&demand)
                    .may_cancel_with(cancel.child_token())
                    .await;
                let segm: Option<&mut R::SegmRef<'_>> = read_res.as_mut().pick_left();
                match segm {
                    Option::Some(segm) => {
                        let mut child = segm.as_segm_ref();
                        // 只搬运请求的长度：段可能比请求的更长。
                        let limit = core::cmp::min(rest, child.least_count());
                        let dst = &mut out[offset..offset + limit];
                        // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同
                        // 对齐、无 niche），且 `dst` 是本地独占的可写切片；
                        // `move_items_to_buff` 只写入其中已初始化的前缀并返回写入
                        // 长度，因此不会读到未初始化内存，也不会越界。
                        let uninit = unsafe {
                            core::slice::from_raw_parts_mut(
                                dst.as_mut_ptr() as *mut core::mem::MaybeUninit<u8>,
                                dst.len(),
                            )
                        };
                        got = unsafe { child.move_items_to_buff(uninit) };
                    }
                    Option::None => {
                        return Result::Err(match read_res.pick_right() {
                            Option::Some(err) => CursorError::Read(err),
                            Option::None => CursorError::PeerClosed,
                        });
                    }
                }
            }
            if got == 0 {
                return Result::Err(CursorError::PeerClosed);
            }
            offset += got;
        }
        Result::Ok(())
    }
}

/// 写出 `bytes` 的全部内容。
///
/// 与读侧对称：借出的段可能比请求的更短，也可能更长；本函数只写入需要的前缀，
/// 未用完的容量在段被回收时归还，因此可以安全地分多次写完。
///
/// # Errors
///
/// 底层写失败 → [`CursorError::Write`]；写完之前对端关闭 →
/// [`CursorError::PeerClosed`]。
pub(crate) async fn write_all_async_<W, K>(
    buff: &mut W,
    bytes: &[u8],
    cancel: K,
) -> Result<(), CursorError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    let mut offset = 0usize;
    while offset < bytes.len() {
        let rest = bytes.len() - offset;
        let demand = Demand::exactly(rest);
        let put;
        {
            let mut write_res = buff
                .write_async(&demand)
                .may_cancel_with(cancel.child_token())
                .await;
            let segm: Option<&mut W::SegmMut<'_>> = write_res.as_mut().pick_left();
            match segm {
                Option::Some(segm) => {
                    put = segm.as_segm_mut().clone_items_from_buff(&bytes[offset..]);
                }
                Option::None => {
                    return Result::Err(match write_res.pick_right() {
                        Option::Some(err) => CursorError::Write(err),
                        Option::None => CursorError::PeerClosed,
                    });
                }
            }
        }
        if put == 0 {
            return Result::Err(CursorError::PeerClosed);
        }
        offset += put;
    }
    Result::Ok(())
}
