//! 子流上的**信封协议**：先告知长度与 digest，再发本体。
//!
//! 每条子流的两个方向各自是一个规整的序列：
//!
//! ```text
//! ENVELOPE{ kind = DATA,    id, len, digest }        32 字节定长头
//! len 字节本体                                        分块产生、分块校验
//! RECEIPT { kind = RECEIPT, id, len, digest, status } 32 字节定长头
//! ```
//!
//! 两种头**共用同一种 32 字节布局**，只用 `kind` / `status` 区分，这样编解码只有一份：
//!
//! | 偏移 | 长度 | 字段 | 说明 |
//! | --- | --- | --- | --- |
//! | 0 | 4 | magic | 恒为 `SMX1`，一道极廉价的错位护栏 |
//! | 4 | 1 | kind | `0 = DATA`，`1 = RECEIPT` |
//! | 5 | 1 | status | 回执的结论：`0 = 一致`，`1 = 不一致`；DATA 恒为 0 |
//! | 6 | 2 | reserved | 必须为 0（协议不许悄悄加字段） |
//! | 8 | 8 | id | 子流序号（= 发起侧分配的临时 local dock 下标） |
//! | 16 | 8 | len | DATA：本体长度；RECEIPT：对端声明的长度 |
//! | 24 | 8 | digest | CRC-32（零扩展成 u64）；RECEIPT 里是**接收方实际算出**的值 |
//!
//! `RECEIPT.digest` 是本协议的关键：它是**接收方**算出来的 digest。发送方拿它与自己
//! 声明的 digest 一比，就证明了「我发的 == 对方收的」。

use core::fmt;

/// 定长头的字节数。
pub const K_HEADER_LEN: usize = 32usize;

/// 头的魔数。
pub const K_MAGIC: [u8; 4] = *b"SMX1";

/// `kind` 取值：数据信封。
pub const K_KIND_DATA: u8 = 0u8;

/// `kind` 取值：回执。
pub const K_KIND_RECEIPT: u8 = 1u8;

/// `status` 取值：接收方算出的 digest 与声明一致。
pub const K_STATUS_OK: u8 = 0u8;

/// `status` 取值：接收方算出的 digest 与声明不一致。
pub const K_STATUS_MISMATCH: u8 = 1u8;

/// 解码失败的原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtoError {
    /// 魔数不符（大概率读错了位置，或对端根本不是本 demo）。
    BadMagic([u8; 4]),
    /// 未知的 `kind`。
    BadKind(u8),
    /// 保留字段不是 0。
    BadReserved([u8; 2]),
}

impl fmt::Display for ProtoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtoError::BadMagic(m) => {
                write!(f, "信封魔数不符：期望 {:?}，实际 {m:?}", K_MAGIC)
            }
            ProtoError::BadKind(k) => write!(f, "未知的信封种类：{k}"),
            ProtoError::BadReserved(r) => write!(f, "信封保留字段必须为 0，实际 {r:?}"),
        }
    }
}

impl std::error::Error for ProtoError {}

/// 一个定长头（DATA 与 RECEIPT 共用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Envelope {
    /// 种类：见 [`K_KIND_DATA`] / [`K_KIND_RECEIPT`]。
    pub kind: u8,
    /// 回执的结论；DATA 恒为 [`K_STATUS_OK`]。
    pub status: u8,
    /// 子流序号。
    pub id: u64,
    /// DATA 为将要发送的本体长度；RECEIPT 为对端声明的本体长度。
    pub len: u64,
    /// DATA 为发送方声明的 digest；RECEIPT 为接收方实际算出的 digest。
    pub digest: u64,
}

impl Envelope {
    /// 造一个 DATA 信封。
    pub fn data(id: u64, len: u64, digest: u32) -> Self {
        Envelope {
            kind: K_KIND_DATA,
            status: K_STATUS_OK,
            id,
            len,
            digest: u64::from(digest),
        }
    }

    /// 造一个 RECEIPT 回执。
    ///
    /// `actual` 是接收方**实际算出**的 digest，`agrees` 是它与声明值是否一致。
    pub fn receipt(id: u64, len: u64, actual: u32, agrees: bool) -> Self {
        Envelope {
            kind: K_KIND_RECEIPT,
            status: if agrees {
                K_STATUS_OK
            } else {
                K_STATUS_MISMATCH
            },
            id,
            len,
            digest: u64::from(actual),
        }
    }

    /// 编码成 32 字节。
    pub fn encode(self) -> [u8; K_HEADER_LEN] {
        let mut out = [0u8; K_HEADER_LEN];
        out[0..4].copy_from_slice(&K_MAGIC);
        out[4] = self.kind;
        out[5] = self.status;
        // out[6..8] 保持 0（保留字段）。
        out[8..16].copy_from_slice(&self.id.to_le_bytes());
        out[16..24].copy_from_slice(&self.len.to_le_bytes());
        out[24..32].copy_from_slice(&self.digest.to_le_bytes());
        out
    }

    /// 从 32 字节解码。
    ///
    /// # Errors
    ///
    /// 魔数不符、`kind` 未知、保留字段非 0 时返回 [`ProtoError`]。
    pub fn decode(buf: &[u8; K_HEADER_LEN]) -> Result<Self, ProtoError> {
        let magic = [buf[0], buf[1], buf[2], buf[3]];
        if magic != K_MAGIC {
            return Err(ProtoError::BadMagic(magic));
        }
        let kind = buf[4];
        if kind != K_KIND_DATA && kind != K_KIND_RECEIPT {
            return Err(ProtoError::BadKind(kind));
        }
        let reserved = [buf[6], buf[7]];
        if reserved != [0u8, 0u8] {
            return Err(ProtoError::BadReserved(reserved));
        }
        let mut id = [0u8; 8];
        id.copy_from_slice(&buf[8..16]);
        let mut len = [0u8; 8];
        len.copy_from_slice(&buf[16..24]);
        let mut digest = [0u8; 8];
        digest.copy_from_slice(&buf[24..32]);
        Ok(Envelope {
            kind,
            status: buf[5],
            id: u64::from_le_bytes(id),
            len: u64::from_le_bytes(len),
            digest: u64::from_le_bytes(digest),
        })
    }

    /// 本条头是不是一条 DATA 信封。
    pub const fn is_data(&self) -> bool {
        self.kind == K_KIND_DATA
    }

    /// 本条头是不是一条 RECEIPT 回执。
    pub const fn is_receipt(&self) -> bool {
        self.kind == K_KIND_RECEIPT
    }

    /// 回执是否报告「一致」。
    pub const fn receipt_agrees(&self) -> bool {
        self.status == K_STATUS_OK
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 校验：DATA 与 RECEIPT 两种头都能按 32 字节编解码往返。
    ///
    /// - 手段：分别构造两种头，`encode` 之后 `decode`。
    /// - 判断：解出的结构体与原文逐字段相等，且 `is_data` / `is_receipt` 判定正确。
    #[test]
    fn envelope_round_trips() {
        let data = Envelope::data(0x1234_5678, 8 * 1024 * 1024, 0xdead_beef);
        assert_eq!(Envelope::decode(&data.encode()).unwrap(), data);
        assert!(data.is_data() && !data.is_receipt());

        let receipt = Envelope::receipt(7, 4096, 0x0102_0304, false);
        let decoded = Envelope::decode(&receipt.encode()).unwrap();
        assert_eq!(decoded, receipt);
        assert!(decoded.is_receipt() && !decoded.is_data());
        assert!(!decoded.receipt_agrees());
    }

    /// 校验：魔数不符、未知种类、保留字段非 0 三种畸形输入都被拒绝。
    ///
    /// - 手段：在一条合法头的编码上分别改掉魔数、`kind`、保留字段。
    /// - 判断：三次 `decode` 分别返回 `BadMagic` / `BadKind` / `BadReserved`。
    #[test]
    fn envelope_rejects_malformed_headers() {
        let good = Envelope::data(1, 2, 3).encode();

        let mut bad_magic = good;
        bad_magic[0] = b'X';
        assert_eq!(
            Envelope::decode(&bad_magic).unwrap_err(),
            ProtoError::BadMagic(*b"XMX1")
        );

        let mut bad_kind = good;
        bad_kind[4] = 9;
        assert_eq!(
            Envelope::decode(&bad_kind).unwrap_err(),
            ProtoError::BadKind(9)
        );

        let mut bad_reserved = good;
        bad_reserved[7] = 1;
        assert_eq!(
            Envelope::decode(&bad_reserved).unwrap_err(),
            ProtoError::BadReserved([0, 1])
        );
    }
}
