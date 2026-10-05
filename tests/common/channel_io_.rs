//! 子流半边的整段读写与 EOF 等待：用例里反复出现的三个公共动作。

use core::mem::MaybeUninit;
use buffex::x_deps::abs_buff::{
    Demand,
    TrBuffRead,
    TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
    error::{ReadErrTag, TrTaggedError},
};

/// 在接收半边等到 EOF（对端 `FIN` 生效）。
///
/// 先尝试读 1 字节：写端关闭且已排空时应当返回 `ConsumerError::Closing`；若对端
/// 的 `FIN` 还没到，`read_async` 会 park 到它到达为止（这正是要验证的行为）。
///
/// 对外可见的理由同 `write_channel_all_`：分层 RPC 用例（`tests/layered_rpc.rs`）
/// 需要同一套「半关闭 → 等 EOF」收尾语义，不该在第二个文件里重写一遍。
pub async fn expect_eof_<R>(rx: &mut R)
where
    R: TrBuffRead<u8>,
{
    let demand = Demand::exactly(1usize);
    let mut outcome = rx.read_async(&demand).await;
    match outcome.as_mut().pick_left() {
        Option::Some(segm) => {
            if segm.least_count() > 0 {
                panic!("半关闭之后仍然读到了数据");
            }
        }
        Option::None => {
            let err = outcome
                .pick_right()
                .expect("IO 结果必须要么是段、要么是错误");
            let tag: ReadErrTag = err.err_tag();
            assert!(
                tag == ReadErrTag::Closing,
                "半关闭之后应当读到 Closing（EOF），实际是 {tag:?}"
            );
        }
    }
}


/// 把 `bytes` 全量写入子流发送半边。
///
/// 与 `abs_buff` 的段语义一致：每次按剩余长度借段、把实际写入量计入段偏移、
/// drop 段提交，直到写完。
///
/// 对外可见是为了让**分层 RPC** 用例（`tests/layered_rpc.rs`）复用同一份搬运
/// 代码：读方向必须用 `unsafe` 的 `move_items_to_buff` 才能把段搬进字节缓冲，
/// 全测试套件只该有一份这样的代码与一份 SAFETY 论证（见 `read_channel_exact_`）。
pub async fn write_channel_all_<W>(tx: &mut W, bytes: &[u8]) -> Result<(), W::Err>
where
    W: TrBuffWrite<u8>,
{
    let mut offset = 0usize;
    while offset < bytes.len() {
        let rest = bytes.len() - offset;
        let demand = Demand::exactly(rest);
        let mut outcome = tx.write_async(&demand).await;
        let put;
        {
            let segm = outcome.as_mut().pick_left();
            match segm {
                Some(segm) => {
                    put = segm.as_segm_mut().clone_items_from_buff(&bytes[offset..]);
                }
                None => {
                    return Err(outcome
                        .pick_right()
                        .expect("IO 结果必须要么是段、要么是错误"));
                }
            }
        }
        if put == 0usize {
            // 段为空说明发送方向已经关闭，继续循环只会自旋。
            break;
        }
        offset += put;
    }
    Ok(())
}


/// 从子流接收半边读满 `out`。
///
/// 与 `write_channel_all_` 对称：段可能比请求更长，只搬走需要的前缀，剩余字节在
/// 段回收时归还缓冲。可见性与 `write_channel_all_` 同理。
pub async fn read_channel_exact_<R>(rx: &mut R, out: &mut [u8]) -> Result<(), R::Err>
where
    R: TrBuffRead<u8>,
{
    let mut offset = 0usize;
    while offset < out.len() {
        let rest = out.len() - offset;
        let demand = Demand::exactly(rest);
        let mut outcome = rx.read_async(&demand).await;
        let got;
        {
            let segm = outcome.as_mut().pick_left();
            match segm {
                Some(segm) => {
                    let mut child = segm.as_segm_ref();
                    let limit = core::cmp::min(rest, child.least_count());
                    let dst = &mut out[offset..offset + limit];
                    // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同、对齐相同（均为
                    // 1），且 `dst` 是本函数独占的可写切片；`move_items_to_buff`
                    // 只会写入其中已初始化的前缀（返回值给出长度）。
                    let uninit = unsafe {
                        core::slice::from_raw_parts_mut(
                            dst.as_mut_ptr() as *mut MaybeUninit<u8>,
                            dst.len(),
                        )
                    };
                    got = unsafe { child.move_items_to_buff(uninit) };
                }
                None => {
                    return Err(outcome
                        .pick_right()
                        .expect("IO 结果必须要么是段、要么是错误"));
                }
            }
        }
        if got == 0usize {
            break;
        }
        offset += got;
    }
    Ok(())
}
