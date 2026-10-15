//! 两条被测路径共用的**最小线格式**与分块工具。
//!
//! ```text
//! +----------------+----------------+------------------------------+
//! | len: u64 LE    | crc32: u32 LE  | 载荷 len 字节（分块发出）      |
//! +----------------+----------------+------------------------------+
//!          8                 4                     len
//! ```
//!
//! # 为什么要有头部
//!
//! 1. **两条路径发同样的字节**：raw（N 条真 UDS）与 smux（复用子流）都发这一份头部 +
//!    同一份载荷，因此「发送的内容完全一致」这件事在线上是可核对的；
//! 2. **接收侧能独立对账**：接收方读完头部后知道该收多少字节，收满之后自己算一遍
//!    CRC-32 与头部比对——本 bench 的每一格都带这条正确性判据，性能数字不会掩盖截断
//!    或串流。
//!
//! 头部只有 12 字节、一格里只发一次，因此它对带宽测量的影响可忽略；而它换来的是
//! 「这一格到底有没有把数据完整送达」的硬证据。

use crate::payload::DigestAccum;

/// 头部长度（8 字节长度 + 4 字节 CRC-32）。
pub const K_HEADER_LEN: usize = 12usize;

/// 一次发送 / 接收的字节数必须是正数且不超过 `u32::MAX`（CRC-32 与 `u64` 长度都不会
/// 溢出，但超大分块本身没有意义，也说明参数写错了）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtoError {
    /// 分块大小为 0。
    ZeroChunk,
    /// 载荷长度与头部声明的长度不一致。
    LengthMismatch {
        /// 头部声明的长度。
        declared: u64,
        /// 实际收到 / 发出的长度。
        actual: u64,
    },
    /// 载荷的 CRC-32 与头部声明的不一致。
    ChecksumMismatch {
        /// 头部声明的值。
        declared: u32,
        /// 实际算出的值。
        actual: u32,
    },
}

impl core::fmt::Display for ProtoError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ProtoError::ZeroChunk => write!(f, "分块大小不能为 0"),
            ProtoError::LengthMismatch { declared, actual } => {
                write!(f, "载荷长度不一致：头部声明 {declared}，实际 {actual}")
            }
            ProtoError::ChecksumMismatch { declared, actual } => write!(
                f,
                "载荷 CRC-32 不一致：头部声明 {declared:#010x}，实际 {actual:#010x}"
            ),
        }
    }
}

impl std::error::Error for ProtoError {}

/// 把 `(len, crc)` 编成 12 字节头部。
pub fn encode_header(len: u64, crc: u32) -> [u8; K_HEADER_LEN] {
    let mut out = [0u8; K_HEADER_LEN];
    out[0..8].copy_from_slice(&len.to_le_bytes());
    out[8..12].copy_from_slice(&crc.to_le_bytes());
    out
}

/// 解析 12 字节头部。
///
/// # Panics
///
/// `bytes` 长度不足 [`K_HEADER_LEN`] 时 panic——调用方（[`crate::transport::read_exact`]）
/// 已经保证读满，长度不足属于本 crate 的内部错误。
pub fn decode_header(bytes: &[u8]) -> (u64, u32) {
    assert!(
        bytes.len() >= K_HEADER_LEN,
        "头部至少 {K_HEADER_LEN} 字节，实际 {}",
        bytes.len()
    );
    let mut len = [0u8; 8];
    len.copy_from_slice(&bytes[0..8]);
    let mut crc = [0u8; 4];
    crc.copy_from_slice(&bytes[8..12]);
    (u64::from_le_bytes(len), u32::from_le_bytes(crc))
}

/// 校验「头部声明的长度」与「实际收到的长度」。
///
/// # Errors
///
/// 两者不等时返回 [`ProtoError::LengthMismatch`]。
pub fn check_len(declared: u64, actual: u64) -> Result<(), ProtoError> {
    if declared == actual {
        Ok(())
    } else {
        Err(ProtoError::LengthMismatch { declared, actual })
    }
}

/// 校验「头部声明的校验和」与「现算的校验和」。
///
/// # Errors
///
/// 两者不等时返回 [`ProtoError::ChecksumMismatch`]。
pub fn check_crc(declared: u32, actual: u32) -> Result<(), ProtoError> {
    if declared == actual {
        Ok(())
    } else {
        Err(ProtoError::ChecksumMismatch { declared, actual })
    }
}

/// 把一个载荷分块切给调用方，并顺带算出整段的校验和。
///
/// 返回「分块迭代器」与「整段 CRC-32」：两者都只依赖载荷本身，因此发送侧可以
/// **先发头部、再逐块发本体**，而头部里的校验和与随后发出的字节必然同源。
pub fn digest_and_chunks<'a>(bytes: &'a [u8], chunk: usize) -> (u32, Chunks<'a>) {
    let mut acc = DigestAccum::new();
    // 分块算 CRC 与一次算完全等价（CRC 是流式的），这里分块的唯一理由是复用同一条
    // 迭代路径，避免「头部校验和」与「本体校验和」用两套代码。
    //
    // **零分块走单块**：`slice::chunks(0)` 会 panic，而 `chunk = 0` 是调用方传错参数
    // 时的退化输入——本 crate 的选择是「当成整段一块」而不是在这里崩（`--chunk` 的
    // 校验会先挡住 0，因此这条只服务于库自身的健壮性）。
    if chunk == 0usize {
        if !bytes.is_empty() {
            acc.update(bytes);
        }
        return (acc.finish(), Chunks { bytes, chunk });
    }
    for part in bytes.chunks(chunk) {
        acc.update(part);
    }
    (acc.finish(), Chunks { bytes, chunk })
}

/// 载荷的定长分块迭代器（`chunk = 0` 时视为整段一块，不 panic）。
#[derive(Debug, Clone, Copy)]
pub struct Chunks<'a> {
    /// 待切分的载荷。
    bytes: &'a [u8],
    /// 每块字节数。
    chunk: usize,
}

impl<'a> Iterator for Chunks<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.is_empty() {
            return None;
        }
        // 零分块是退化输入：把它当成「整段一块」。`bytes.chunks(0)` 会 panic，因此这里
        // 自己算切分点（调用方本来也会用 `--chunk` 校验挡住 0，这里只是不让它崩）。
        let take = if self.chunk == 0usize {
            self.bytes.len()
        } else {
            core::cmp::min(self.chunk, self.bytes.len())
        };
        let (head, tail) = self.bytes.split_at(take);
        self.bytes = tail;
        Some(head)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试头部编解码是**往返一致**的（小端、无对齐假设）。
    /// - 手段：对若干边界值（0、1、`u32::MAX`、`u64::MAX`）各编一次再解一次。
    /// - 判断：解出的 `(len, crc)` 与输入完全相同。
    ///
    /// Checks the 12-byte header round-trips.
    /// - Method: encode and decode several boundary values.
    /// - Assertion: the decoded pair equals the encoded one.
    #[test]
    fn header_round_trips_() {
        for (len, crc) in [
            (0u64, 0u32),
            (1u64, 1u32),
            (u32::MAX as u64, 0xDEAD_BEEFu32),
            (u64::MAX, u32::MAX),
        ] {
            let encoded = encode_header(len, crc);
            assert_eq!(encoded.len(), K_HEADER_LEN);
            assert_eq!(decode_header(&encoded), (len, crc));
        }
    }

    /// 测试校验函数对「一致 / 不一致」两种输入各给出正确结论。
    /// - 手段：分别用相等的与不相等的 `(declared, actual)` 调用 `check_len` / `check_crc`。
    /// - 判断：相等时 `Ok`，不等时返回携带两端数值的对应错误变体。
    ///
    /// Checks the verifiers accept equal values and name the mismatch otherwise.
    /// - Method: call `check_len` / `check_crc` with equal and unequal pairs.
    /// - Assertion: `Ok` on equal, and the matching error variant carrying both
    ///   values on unequal.
    #[test]
    fn verifiers_report_mismatch_() {
        assert!(check_len(7u64, 7u64).is_ok());
        assert_eq!(
            check_len(7u64, 8u64),
            Err(ProtoError::LengthMismatch {
                declared: 7u64,
                actual: 8u64
            })
        );
        assert!(check_crc(9u32, 9u32).is_ok());
        assert_eq!(
            check_crc(9u32, 10u32),
            Err(ProtoError::ChecksumMismatch {
                declared: 9u32,
                actual: 10u32
            })
        );
    }

    /// 测试分块迭代器覆盖整段、不重不漏，且载荷校验和与逐块累加一致。
    /// - 手段：对 1000 字节分别用 64 / 333 / 4096 三种分块切开，累加各块长度与 CRC。
    /// - 判断：各块长度和等于 1000；三条分块路径的 CRC 互不相等的地方不存在——它们
    ///   必须全部相等且等于 `digest_and_chunks` 给出的值。
    ///
    /// Checks the chunk iterator tiles the payload exactly.
    /// - Method: split 1000 bytes with three different chunk sizes and accumulate.
    /// - Assertion: the lengths sum to 1000 and every path yields the same CRC as
    ///   `digest_and_chunks`.
    #[test]
    fn chunks_tile_payload_and_agree_on_digest_() {
        let bytes: Vec<u8> = (0..1000usize).map(|i| (i % 251usize) as u8).collect();
        let mut baseline: Option<u32> = None;
        for chunk in [64usize, 333usize, 4096usize] {
            let (crc, parts) = digest_and_chunks(&bytes, chunk);
            let mut total = 0usize;
            let mut acc = DigestAccum::new();
            for part in parts {
                total += part.len();
                acc.update(part);
            }
            assert_eq!(total, bytes.len());
            assert_eq!(acc.finish(), crc);
            match baseline {
                Some(want) => assert_eq!(crc, want),
                None => baseline = Some(crc),
            }
        }
    }

    /// 测试空载荷与零分块两种退化输入不 panic。
    /// - 手段：空切片配 64 字节分块、非空切片配 0 字节分块各切一遍。
    /// - 判断：前者一个块都不产出；后者产出整整一块（等于整段）。
    ///
    /// Checks the degenerate inputs do not panic.
    /// - Method: an empty payload and a zero chunk size.
    /// - Assertion: the empty payload yields no chunk; the zero chunk yields the
    ///   whole payload.
    #[test]
    fn degenerate_payloads_do_not_panic_() {
        let (_, parts) = digest_and_chunks(&[], 64usize);
        assert_eq!(parts.count(), 0usize);

        let bytes = [1u8, 2u8, 3u8];
        let (_, parts) = digest_and_chunks(&bytes, 0usize);
        let collected: Vec<&[u8]> = parts.collect();
        assert_eq!(collected.len(), 1usize);
        assert_eq!(collected[0], &bytes[..]);
    }
}
