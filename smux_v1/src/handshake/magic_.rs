//! 握手帧的 magic 常量。
//!
//! 四种握手帧各有一条固定的 4 字节 magic（见 [`crate::handshake`] 模块文档 §5）。

const MAGIC_FIELD_SIZE: usize = 4;
pub(crate) type MagicField = [u8; MAGIC_FIELD_SIZE];

/// `INVITE` 帧的 magic：`5F 1B 01 69`。
///
/// 发起方发送的第一帧，携带其协商条目，见模块级文档 §5。
pub const K_INVITE_MAGIC: MagicField = [95, 27, 1, b'i'];

/// `ACCEPT` 帧的 magic：`5F 1B 01 41`。
///
/// 等待方对 `INVITE` 的接受应答，条目区为补全后的完整协商结果，见 §5 与 §7.2。
pub const K_ACCEPT_MAGIC: MagicField = [95, 27, 1, b'A'];

/// `REJECT` 帧的 magic：`5F 1B 01 4A`。
///
/// 协商失败时由任一方发送；v1 的条目区必须为空，见 §7.5。
pub const K_REJECT_MAGIC: MagicField = [95, 27, 1, b'J'];

/// `CONFRM` 帧的 magic：`5F 1B 01 63`。
///
/// 发起方用它提交完整协商结果；等待方用它发送空条目区的最终确认，见 §5。
pub const K_CONFRM_MAGIC: MagicField = [95, 27, 1, b'c'];
