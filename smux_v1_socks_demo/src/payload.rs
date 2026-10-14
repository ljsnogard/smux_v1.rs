//! 确定性载荷与 digest。
//!
//! 两端**各自**按同样的 `(种子, 子流序号, 哪一侧)` 把同一串字节算出来：发送方发它、
//! 接收方拿它逐字节比对。因此本实验不需要传文件、也不依赖磁盘 IO——「数据一模一样」
//! 是算出来的，不是约定出来的。
//!
//! 两个易错点在这里一次性解决：
//!
//! 1. **分块无关**：生成器内部保留一个未用完的 8 字节字，因此「一次要 32 KiB」与
//!    「一次要 4096 字节」产生的字节流完全相同（否则 `--chunk` 一改就全错）；
//! 2. **先 digest 后本体**：`digest_of` 用与发送侧**同一个**生成器流一遍算出 CRC-32，
//!    因此「先告知的 digest」与「随后发出的本体」必然同源。

use smux_v1::x_deps::crc::{CRC_32_ISO_HDLC, Crc};

/// 一次填多少字节去算 digest（与传输分块无关，只是取个整数）。
const K_DIGEST_CHUNK: usize = 64usize * 1024usize;

/// 确定性载荷生成器。
#[derive(Debug, Clone)]
pub struct PayloadSrc {
    /// xorshift64 的状态；恒不为 0。
    state_: u64,
    /// 还剩多少字节没产出。
    remaining_: u64,
    /// 当前 8 字节字。
    word_: [u8; 8],
    /// `word_` 里已经消费到哪一字节。
    word_off_: usize,
}

impl PayloadSrc {
    /// 按 `(种子, 子流序号, 侧别)` 造一台生成器，产出 `len` 字节。
    ///
    /// `side` 取 0（发起侧）或 1（接受侧）：两侧的载荷因此**必然不同**。
    pub fn new(seed: u64, id: u64, side: u64, len: u64) -> Self {
        let state = seed_of_(seed, id, side);
        PayloadSrc {
            state_: state,
            remaining_: len,
            word_: [0u8; 8],
            word_off_: 8usize,
        }
    }

    /// 还剩多少字节没产出。
    pub const fn remaining(&self) -> u64 {
        self.remaining_
    }

    /// 产出接下来的若干字节（最多不超过剩余量），返回写入的字节数。
    ///
    /// 返回值等于 `min(out.len(), remaining())`；调用方按返回值取用前缀。
    pub fn fill(&mut self, out: &mut [u8]) -> usize {
        let want = core::cmp::min(out.len() as u64, self.remaining_) as usize;
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
        self.remaining_ -= want as u64;
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

/// 算出与 [`PayloadSrc`] 同源的 CRC-32 digest。
pub fn digest_of(seed: u64, id: u64, side: u64, len: u64) -> u32 {
    let mut src = PayloadSrc::new(seed, id, side, len);
    let crc = Crc::<u32>::new(&CRC_32_ISO_HDLC);
    let mut d = crc.digest();
    let mut buf = vec![0u8; K_DIGEST_CHUNK];
    while src.remaining() > 0u64 {
        let n = src.fill(&mut buf);
        d.update(&buf[..n]);
    }
    d.finalize()
}

/// 由 `(种子, 子流序号, 侧别)` 混出一个非零初态。
fn seed_of_(seed: u64, id: u64, side: u64) -> u64 {
    let mut x =
        seed ^ id.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ side.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    // splitmix64 的两步混淆：把相邻的 id / side 打散成互不相似的初态。
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    if z == 0u64 { 0x9E37_79B9_7F4A_7C15 } else { z }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 校验：同一参数下的字节流与**取用分块大小无关**。
    ///
    /// - 手段：同一台生成器用两种分块（7 字节与 4096 字节）各取满 10000 字节。
    /// - 判断：两条字节流逐字节相等——这条一旦不成立，改 `--chunk` 就会让两头对不上。
    #[test]
    fn stream_is_chunk_independent() {
        let total = 10_000usize;
        let mut a = Vec::with_capacity(total);
        let mut src = PayloadSrc::new(42, 3, 0, total as u64);
        let mut buf = [0u8; 7];
        while src.remaining() > 0 {
            let n = src.fill(&mut buf);
            a.extend_from_slice(&buf[..n]);
        }

        let mut b = Vec::with_capacity(total);
        let mut src = PayloadSrc::new(42, 3, 0, total as u64);
        let mut buf = vec![0u8; 4096];
        while src.remaining() > 0 {
            let n = src.fill(&mut buf);
            b.extend_from_slice(&buf[..n]);
        }

        assert_eq!(a, b);
        assert_eq!(a.len(), total);
    }

    /// 校验：`digest_of` 与「流式取出的字节」算出的 CRC 一致，且两侧载荷不同。
    ///
    /// - 手段：对 `(seed=1, id=5, side=0/1)` 分别调 `digest_of`；另外把 `side=0` 的
    ///   字节流单独用同一个 CRC 参数算一遍。
    /// - 判断：两个 `digest_of` 互不相等（左右两侧载荷必定不同），且 `side=0` 的
    ///   `digest_of` 等于就地流式计算的结果。
    #[test]
    fn digest_matches_stream_and_differs_by_side() {
        let len = 100_000u64;
        let d0 = digest_of(1, 5, 0, len);
        let d1 = digest_of(1, 5, 1, len);
        assert_ne!(d0, d1, "同一子流两侧的载荷必须不同");

        let mut src = PayloadSrc::new(1, 5, 0, len);
        let crc = Crc::<u32>::new(&CRC_32_ISO_HDLC);
        let mut d = crc.digest();
        let mut buf = vec![0u8; 3333];
        while src.remaining() > 0 {
            let n = src.fill(&mut buf);
            d.update(&buf[..n]);
        }
        assert_eq!(d.finalize(), d0);
    }

    /// 校验：`fill` 不会越过声明的长度，剩余量随之递减到 0。
    ///
    /// - 手段：要 100 字节，但用 64 字节的缓冲反复取。
    /// - 判断：首次取回 64、剩余 36；再次取回 36、剩余 0；第三次取回 0。
    #[test]
    fn fill_stops_at_len() {
        let mut src = PayloadSrc::new(9, 9, 9, 100);
        let mut buf = [0u8; 64];
        assert_eq!(src.fill(&mut buf), 64);
        assert_eq!(src.remaining(), 36);
        assert_eq!(src.fill(&mut buf), 36);
        assert_eq!(src.remaining(), 0);
        assert_eq!(src.fill(&mut buf), 0);
    }
}
