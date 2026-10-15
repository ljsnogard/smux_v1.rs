//! 确定性载荷：两条被测路径**发的是逐字节相同的一串数据**。
//!
//! # 为什么要「生成」而不是「读文件」
//!
//! 本 bench 的两条路径（raw 的 N 条真 UDS、smux 的 1 条 UDS + N 条复用子流）要满足
//! 「发送的内容完全一致」。最省事也最不容易出错的落地方式是：**按 `(种子, 序号)` 算出来**。
//! 于是这件事与磁盘 IO、文件位置、两端的一致性都无关，只与一行参数有关。
//!
//! # 生成器写成了**分块无关**的
//!
//! 内部保留一个未用完的 8 字节字，因此「一次要 64 KiB」与「一次要 4096 字节」得到的
//! 字节流完全相同。这一条很重要：本 bench 要扫不同的 `--chunk`，一旦分块会改变字节流，
//! 「两条路径发的内容一致」就只在同一 `--chunk` 下成立——那是隐形的坑。
//!
//! # 校验和
//!
//! 用 `smux_v1` 已经转出的 `crc` crate（CRC-32/ISO-HDLC）。发送方**先把校验和写进头部、
//! 再发本体**；接收方收满之后自己算一遍比对。因此「收到的东西对不对」是每一格都在
//! 现算的，不是靠参数约定。

use smux_v1::x_deps::crc::{CRC_32_ISO_HDLC, Crc, Digest};

/// 本 bench 用的 CRC-32/ISO-HDLC 实例（`Crc::new` 是 `const fn`，因此可以放在 static 里；
/// 放 static 而不是每次现造，是为了让 [`Digest`] 能借用它——`Digest<'a, _>` 持有
/// `&'a Crc`，借一个局部变量的活法会立刻撞上生命周期）。
static BENCH_CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

/// 默认的**每个客户端 / 每条子流**发送字节数（64 MiB）。
pub const K_DEFAULT_BYTES_PER_CLIENT: u64 = 64u64 * 1024u64 * 1024u64;

/// 默认的发送分块字节数（64 KiB）。
pub const K_DEFAULT_CHUNK: usize = 64usize * 1024usize;

/// 默认的环容量（字节）：同时作为 smux 子流环与适配器传输环的容量。
///
/// 取值与 `smux_v1` 的 `K_TRANSPORT_RING_CAPACITY` 一致（64 KiB）。**不变量**：
/// 它必须 ≥ 协商的 `max_packet_size`，否则数据报会被永久推迟；本 bench 只走子流，
/// 但保持同一个默认值可以避免读者误以为两者无关。
pub const K_DEFAULT_RING_CAP: usize = 64usize * 1024usize;

/// 确定性载荷生成器（xorshift64* 流）。
#[derive(Debug, Clone)]
pub struct PayloadSrc {
    /// xorshift64* 的状态；恒不为 0。
    state_: u64,
    /// 还剩多少字节没产出。
    remaining_: usize,
    /// 当前 8 字节字。
    word_: [u8; 8],
    /// `word_` 里已经消费到哪一字节。
    word_off_: usize,
}

impl PayloadSrc {
    /// 按 `(种子, 序号)` 造一台产出 `len` 字节的生成器。
    ///
    /// `len` 是 `u64`（命令行与报告都用 `u64` 表达字节数），内部按 `usize` 计数——
    /// 本 bench 的载荷只有几十 MiB，远超 32 位平台上限的场景不存在。
    pub fn new(seed: u64, id: u64, len: u64) -> Self {
        PayloadSrc {
            state_: seed_of_(seed, id),
            remaining_: len as usize,
            word_: [0u8; 8],
            word_off_: 8usize,
        }
    }

    /// 还剩多少字节没产出。
    pub const fn remaining(&self) -> usize {
        self.remaining_
    }

    /// 产出接下来的若干字节（最多不超过剩余量），返回写入的字节数。
    ///
    /// 返回值恒等于 `min(out.len(), remaining())`；调用方按返回值取用前缀。
    pub fn fill(&mut self, out: &mut [u8]) -> usize {
        let want = core::cmp::min(out.len(), self.remaining_);
        let mut off = 0usize;
        while off < want {
            if self.word_off_ == 8usize {
                self.word_ = self.next_word_().to_le_bytes();
                self.word_off_ = 0usize;
            }
            let take = core::cmp::min(8usize - self.word_off_, want - off);
            out[off..off + take]
                .copy_from_slice(&self.word_[self.word_off_..self.word_off_ + take]);
            self.word_off_ += take;
            off += take;
        }
        self.remaining_ -= want;
        want
    }

    /// 推进一步 xorshift64*。
    fn next_word_(&mut self) -> u64 {
        let mut x = self.state_;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state_ = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// 一次性把 `len` 字节的确定性载荷算进 `Vec`（发送侧用；接收侧只用于校验和）。
pub fn payload_of(seed: u64, id: u64, len: u64) -> Vec<u8> {
    let mut src = PayloadSrc::new(seed, id, len);
    let mut out = vec![0u8; len as usize];
    let total = out.len();
    let mut off = 0usize;
    while off < total {
        let n = src.fill(&mut out[off..]);
        if n == 0usize {
            break;
        }
        off += n;
    }
    debug_assert_eq!(off, total, "生成器必须恰好产出声明长度的字节");
    out
}

/// 与 [`PayloadSrc`] 同源的 CRC-32 校验和。
///
/// 发送侧把它写进头部（因此接收侧不必自己生成载荷就能对账）；接收侧也可以就地流式
/// 累加——本 bench 两者都做（头部里的值与接收后现算的值必须相等）。
pub fn digest_of(seed: u64, id: u64, len: u64) -> u32 {
    let mut src = PayloadSrc::new(seed, id, len);
    let mut d = BENCH_CRC.digest();
    let mut buf = vec![0u8; 64usize * 1024usize];
    while src.remaining() > 0usize {
        let n = src.fill(&mut buf);
        d.update(&buf[..n]);
    }
    d.finalize()
}

/// 增量校验和累加器（接收侧一边读一边算，不额外占一块载荷内存）。
///
/// 不派生 `Debug`：底层 `crc::Digest` 没有实现它，而本类型没有需要打印的调试信息。
#[derive(Clone)]
pub struct DigestAccum {
    /// 底层 CRC 摘要（借的是 [`BENCH_CRC`] 这个 `static`，因此生命周期是 `'static`）。
    digest_: Digest<'static, u32>,
}

impl DigestAccum {
    /// 造一个空累加器。
    pub fn new() -> Self {
        DigestAccum {
            digest_: BENCH_CRC.digest(),
        }
    }

    /// 追加一段字节。
    pub fn update(&mut self, bytes: &[u8]) {
        self.digest_.update(bytes);
    }

    /// 收口并取回校验和（消费自己，避免重复收口）。
    pub fn finish(self) -> u32 {
        self.digest_.finalize()
    }
}

impl Default for DigestAccum {
    fn default() -> Self {
        DigestAccum::new()
    }
}

/// 由 `(种子, 序号)` 混出一个非零初态（splitmix64 的两步混淆）。
fn seed_of_(seed: u64, id: u64) -> u64 {
    let mut x = seed ^ id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    if z == 0u64 {
        0x9E37_79B9_7F4A_7C15
    } else {
        z
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试载荷字节流与取用分块**无关**。
    /// - 手段：同一 `(seed, id, len)` 分别用 7 字节与 4096 字节两种分块取满 10 000 字节。
    /// - 判断：两条流逐字节相等，且长度都对；否则改 `--chunk` 就会让两条路径的载荷分叉。
    ///
    /// Checks the payload stream is independent of the chunk size.
    /// - Method: fill 10 000 bytes twice, once in 7-byte chunks and once in 4096-byte
    ///   chunks, from the same `(seed, id, len)`.
    /// - Assertion: both streams are byte-identical and have the expected length.
    #[test]
    fn payload_stream_is_chunk_independent_() {
        let total = 10_000u64;
        let filler = |chunk: usize| -> Vec<u8> {
            let mut out = Vec::with_capacity(total as usize);
            let mut src = PayloadSrc::new(42u64, 3u64, total);
            let mut buf = vec![0u8; chunk];
            loop {
                let n = src.fill(&mut buf);
                if n == 0usize {
                    break;
                }
                out.extend_from_slice(&buf[..n]);
            }
            out
        };
        let a = filler(7usize);
        let b = filler(4096usize);
        assert_eq!(a.len(), total as usize);
        assert_eq!(a, b);
    }

    /// 测试增量累加器与一次性生成器的校验和一致，且不同序号载荷不同。
    /// - 手段：对 `(seed=1, id=5)` 调一次 `digest_of`；再用 `payload_of` + `DigestAccum`
    ///   现算一遍；同时算 `id=6` 的校验和。
    /// - 判断：前两者相等；`id=5` 与 `id=6` 不等（序号必须真的进载荷）。
    ///
    /// Checks the incremental accumulator matches the one-shot digest.
    /// - Method: compare `digest_of` against `DigestAccum` over `payload_of`, and also
    ///   compute the digest of a different id.
    /// - Assertion: the first two agree and the two ids differ.
    #[test]
    fn incremental_digest_matches_one_shot_() {
        let len = 100_000u64;
        let one_shot = digest_of(1u64, 5u64, len);

        let bytes = payload_of(1u64, 5u64, len);
        assert_eq!(bytes.len(), len as usize);
        let mut acc = DigestAccum::new();
        for chunk in bytes.chunks(3333usize) {
            acc.update(chunk);
        }
        assert_eq!(acc.finish(), one_shot);
        assert_ne!(digest_of(1u64, 6u64, len), one_shot);
    }

    /// 测试 `PayloadSrc::fill` 不会越过声明的长度。
    /// - 手段：要 100 字节，用 64 字节缓冲反复取。
    /// - 判断：依次取回 64 / 36 / 0，剩余量随之递减到 0。
    ///
    /// Checks `fill` never exceeds the declared length.
    /// - Method: request 100 bytes with a 64-byte buffer.
    /// - Assertion: 64, then 36, then 0 bytes are produced.
    #[test]
    fn fill_stops_at_declared_len_() {
        let mut src = PayloadSrc::new(9u64, 9u64, 100u64);
        let mut buf = [0u8; 64];
        assert_eq!(src.fill(&mut buf), 64usize);
        assert_eq!(src.remaining(), 36usize);
        assert_eq!(src.fill(&mut buf), 36usize);
        assert_eq!(src.remaining(), 0usize);
        assert_eq!(src.fill(&mut buf), 0usize);
    }
}
