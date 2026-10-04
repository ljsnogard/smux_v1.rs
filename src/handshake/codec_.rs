//! 握手帧的编解码：直接针对 [`TrBuffRead`] / [`TrBuffWrite`] 工作。
//!
//! 本模块把 [`crate::handshake`] 模块文档 §4、§6 的线格式实现为两组操作：
//!
//! - [`FrameReader`]：先读 `magic` 与**算法预告**，随后**逐条**产出协商条目，
//!   读到校验头即读取 `crc` 并做一票否决的校验；
//! - [`write_frame_`]：逐字段把一帧写进字节流，边写边增量计算 `crc`。
//!
//! # 为什么可以「不保存整帧」
//!
//! 帧没有长度字段（见模块文档 §4）：`magic` 之后是 1 字节**算法预告**，随后是
//! 任意多个条目，帧尾是 1 字节**校验头**（定界符，宣告条目区结束）与
//! 2/3/4 字节 `crc`。由此得到两条关键性质：
//!
//! 1. **单趟增量校验**：算法在帧首就已确定，读到哪个字节就喂进同一个校验状态，
//!    到帧尾 `finalize` 比较即可——既不需要同时维护多个候选算法，也不需要保存
//!    整帧；
//! 2. **逐条目流式解析**：每个条目的宽度只由它自己的 `header` 决定，因此读到
//!    一条就能立刻解析、立刻交给协商方，内存占用与帧长、条目数量**无关**。
//!
//! 内存因此与**帧长、条目数量都无关**：
//!
//! - 基础项是定长整数，最多 8 字节，直接在栈上解码（`decode_value_`），
//!   **不留存原始字节**；5 个基础键各有一个定长槽位；
//! - 扩展条目在 v1 中是保留键，读到键就拒绝，因此目前**没有任何**按声明长度
//!   申请缓冲的路径。将来启用时会先校验「声明长度 ≤ 内部上限」再准备等长
//!   缓冲，这一步必须在读负载**之前**发生（`dev-notes.md` D2）；
//! - 校验只维护一个 [`CrcDigest`]（增量状态由 `crc` crate 提供，本模块不实现
//!   CRC 数学）。
//!
//! 结论：本模块不存在任何「按帧长申请缓冲」或「先收完整帧」的代码路径。
//!
//! 单个条目的 `header` 与 `value` 宽度全部取自 [`NegotiationKey`] 与
//! [`NegotiationValType`]，本模块不重复描述任何「键 ↔ 字节数」的对应关系；
//! 算法预告与 [`NegotiationKey::Checksum`] 共用同一套头字节编码（见
//! [`HandshakeChecksum`]）。

use core::time::Duration;

use abs_async_iter::TrAsyncIterator;
use abs_buff::{
    TrBuffRead, TrBuffWrite, gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use buffex::x_deps::abs_buff;

use crate::wire_io_::{CursorError, ReadCursor, write_all_async_};
use crate::handshake::{
    MagicField,
    opts::{
        BasicOpts, K_BASIC_KEY_COUNT, NegotiationBasicEntry, NegotiationEntry, NegotiationKey,
        NegotiationValType,
    },
};

/// CRC-16/XMODEM 的规范实例。
///
/// `crc::Crc` 内含 256 项查表数据（`Crc<u16>` 520 字节、`Crc<u32>` 1032 字节）。
/// 把它做成 `static` 有两个好处：
///
/// 1. [`HandshakeChecksum`] 只持有引用，枚举从约 1 KB 缩到 16 字节，也不会在
///    每次 `try_new` / 克隆时复制整张表；
/// 2. 增量校验拿到的 [`crc::Digest`] 生命周期是 `'static`，于是它能安全地跨
///    `await` 存放在读状态机里（`dev-notes.md` D3 方案 1）。
static HANDSHAKE_CRC16: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_XMODEM);

/// CRC-24/BLE 的规范实例；见 [`HANDSHAKE_CRC16`]。
static HANDSHAKE_CRC24: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_24_BLE);

/// CRC-32/ISO-HDLC 的规范实例；见 [`HANDSHAKE_CRC16`]。
static HANDSHAKE_CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

/// 握手帧默认使用的校验算法：CRC-16/XMODEM。
///
/// 校验算法逐帧自声明（模块文档 §3），需要别的算法时由调用方在 [`write_frame_`]
/// 一层指定。
pub(crate) const K_DEFAULT_CHECKSUM: HandshakeChecksum = HandshakeChecksum::Crc16(&HANDSHAKE_CRC16);

/// 握手帧使用的校验算法。
///
/// 算法在帧中由**同一个头字节**声明两次：帧首的**算法预告**（模块文档 §4.2）
/// 与帧尾的**校验头**（§4.3，定界符）。该字节与
/// [`NegotiationKey::Checksum`] 条目的头字节**编码方式相同**（高半字节是
/// `val_type`，低半字节是键 `0x0C`），因此本类型与 [`NegotiationValType`]
/// 一一对应：
///
/// | 枚举 | `val_type` | 预告 / 校验头字节 | 算法 | `crc` 长度 |
/// | --- | --- | --- | --- | --- |
/// | [`HandshakeChecksum::Crc16`] | `BeU16` | `0x1C` | CRC-16/XMODEM | 2 |
/// | [`HandshakeChecksum::Crc24`] | `BeU24` | `0x2C` | CRC-24/BLE | 3 |
/// | [`HandshakeChecksum::Crc32`] | `BeU32` | `0x3C` | CRC-32/ISO-HDLC | 4 |
///
/// 两处声明**必须一致**：不一致时整帧按 `MalformedBody` 拒绝。
///
/// 校验范围见模块文档 §4：从 `magic` 首字节起，到 `crc` 之前（含算法预告与
/// 校验头）的全部字节；`crc` 自身不计入。
///
/// # 载荷
///
/// 每个变体携带的是该算法的**规范实例引用**（`&'static crc::Crc<W>`），而不是
/// `Crc` 值本身。`Crc` 内含整张 256 项查表数据（`Crc<u16>` 520 字节、
/// `Crc<u32>` 1032 字节），改为引用后本枚举只有 16 字节（`Clone + Copy`），
/// 且增量校验可以直接拿到 `'static` 的 [`crc::Digest`]。
///
/// # Examples
///
/// ```
/// use smux_v1::handshake::HandshakeChecksum;
/// use smux_v1::handshake::opts::NegotiationValType;
///
/// // 帧首声明的 `val_type` 决定校验算法与校验码长度。
/// let algo = HandshakeChecksum::try_new(NegotiationValType::BeU24).unwrap();
/// assert_eq!(algo.checksum_len(), 3usize);
/// assert_eq!(u8::from(algo.header()), u8::from(NegotiationValType::BeU24));
///
/// // 只有 16 / 24 / 32 位校验码是合法算法。
/// assert!(HandshakeChecksum::try_new(NegotiationValType::BeU64).is_none());
/// ```
#[derive(Clone, Copy)]
pub enum HandshakeChecksum {
    /// CRC-16/XMODEM，校验码 2 字节。
    Crc16(&'static crc::Crc<u16>),

    /// CRC-24/BLE，校验码 3 字节。
    Crc24(&'static crc::Crc<u32>),

    /// CRC-32/ISO-HDLC，校验码 4 字节。
    Crc32(&'static crc::Crc<u32>),
}

impl core::fmt::Debug for HandshakeChecksum {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "HandshakeChecksum::{}({} 字节)",
            match self.header() {
                NegotiationValType::BeU16 => "Crc16",
                NegotiationValType::BeU24 => "Crc24",
                NegotiationValType::BeU32 => "Crc32",
                _ => "Unknown",
            },
            self.checksum_len()
        )
    }
}

impl HandshakeChecksum {
    /// 把算法预告 / 校验头字节声明的 [`NegotiationValType`] 映射为校验算法。
    ///
    /// `BeU8` 与 `BeU64` 在 v1 中不是合法的校验尾类型，返回 `None`。
    pub const fn try_new(val_type: NegotiationValType) -> Option<Self> {
        match val_type {
            NegotiationValType::BeU16 => Option::Some(HandshakeChecksum::Crc16(&HANDSHAKE_CRC16)),
            NegotiationValType::BeU24 => Option::Some(HandshakeChecksum::Crc24(&HANDSHAKE_CRC24)),
            NegotiationValType::BeU32 => Option::Some(HandshakeChecksum::Crc32(&HANDSHAKE_CRC32)),
            _ => Option::None,
        }
    }

    /// 把帧首**算法预告** / 帧尾**校验头**的原始字节解析为校验算法。
    ///
    /// 合法取值只有 `0x1C` / `0x2C` / `0x3C` 三种：低半字节必须是
    /// [`NegotiationKey::Checksum`]，高半字节必须是 16/24/32 位宽度。其余取值
    /// （含 CRC-8 `0x0C`、CRC-64 `0x4C`）返回 `None`，由调用方按
    /// `UnsupportedOption` 处理（模块文档 §4.1）。
    pub fn try_header(header: u8) -> Option<Self> {
        if header & NegotiationKey::MASK != NegotiationKey::Checksum as u8 {
            return Option::None;
        }
        let val_type = NegotiationValType::try_from(header).ok()?;
        HandshakeChecksum::try_new(val_type)
    }

    /// 本算法写在校验尾头字节中的 `val_type`。
    pub const fn header(&self) -> NegotiationValType {
        match self {
            HandshakeChecksum::Crc16(_) => NegotiationValType::BeU16,
            HandshakeChecksum::Crc24(_) => NegotiationValType::BeU24,
            HandshakeChecksum::Crc32(_) => NegotiationValType::BeU32,
        }
    }

    /// 校验码的字节数。
    ///
    /// 直接委托 [`NegotiationValType::value_len`]，避免在长度上再维护一份
    /// 对应关系。
    pub const fn checksum_len(&self) -> usize {
        self.header().value_len()
    }

    /// 校验尾的完整头字节（`key == Checksum` 与 `val_type` 的组合）。
    pub(crate) const fn header_byte_(&self) -> u8 {
        compose_header_(NegotiationKey::Checksum, self.header())
    }
}

/// 增量 CRC 状态。
///
/// 帧首的算法预告让接收方在读完 `magic` 之后就能选定算法，因此整帧只需维护
/// **一个**校验状态：读到哪个字节就喂哪个字节（模块文档 §11）。
///
/// 这里直接使用 `crc` crate 的增量 API [`crc::Digest`]，不再自己实现 CRC 数学。
/// `Digest` 借用了 `Crc` 实例，而算法要到运行时才确定，所以用一个小枚举承载三
/// 种宽度；由于 [`HandshakeChecksum`] 持有的是 `&'static Crc`，这个借用就是
/// `'static`，读状态机可以安全地在 `await` 之间持有它（`dev-notes.md` D3 方案 1）。
#[derive(Clone)]
pub(crate) struct CrcDigest(CrcDigestInner);

/// 三种算法各自的增量状态。
///
/// `crc` crate 把 `Crc<u16>` 与 `Crc<u32>` 做成了不同类型，因此只能用枚举承载。
/// `Digest` 的大小是「一个引用 + 一个寄存器」（实测 16 字节）。
#[derive(Clone)]
enum CrcDigestInner {
    W16(crc::Digest<'static, u16>),
    W32(crc::Digest<'static, u32>),
}

impl CrcDigest {
    /// 按 `checksum` 指定的算法初始化。
    pub(crate) fn new_(checksum: &HandshakeChecksum) -> Self {
        let inner = match *checksum {
            HandshakeChecksum::Crc16(c) => CrcDigestInner::W16(c.digest()),
            HandshakeChecksum::Crc24(c) | HandshakeChecksum::Crc32(c) => {
                CrcDigestInner::W32(c.digest())
            }
        };
        CrcDigest(inner)
    }

    /// 把 `bytes` 依次计入校验状态。
    pub(crate) fn update_(&mut self, bytes: &[u8]) {
        match &mut self.0 {
            CrcDigestInner::W16(d) => d.update(bytes),
            CrcDigestInner::W32(d) => d.update(bytes),
        }
    }

    /// 结束计算并返回校验码数值；宽度与 [`HandshakeChecksum::checksum_len`]
    /// 一致（高位为 0）。
    ///
    /// [`crc::Digest::finalize`] 会消耗自身，而这里只借到 `&self`，因此先克隆一
    /// 份再 finalize。`Digest` 只是「引用 + 寄存器」，克隆代价与一次读取相当，
    /// 且 CRC 状态本来就很小。
    pub(crate) fn finalize_(&self) -> u32 {
        match &self.0 {
            CrcDigestInner::W16(d) => d.clone().finalize() as u32,
            CrcDigestInner::W32(d) => d.clone().finalize(),
        }
    }
}

/// 读出**下一个条目**，供 [`TrAsyncIterator::next_async`] 使用。
///
/// 这是条目流的 step 函数：它被 [`gen_may_cancel_future`] 展开成同时实现
/// [`IntoFuture`] 与 [`TrMayCancel`] 的 future 类型 [`NextEntryAsync`]。
///
/// # 生命周期
///
/// - `'s`：条目对外暴露的生命周期，取**外层借用** `&'s mut FrameReader`；
/// - `'f`：读缓冲的借用；`'r`：本次读取对 `reader` 的借用。
///
/// [`NegotiationEntry`] 对生命周期协变，且 `'f: 's`，所以读状态机产出的
/// `NegotiationEntry<'f>` 可以收窄成 `NegotiationEntry<'s>`。这样协商器拿到的条目
/// 与条目流借用同一个（较短的）生命周期，调用方在协商返回后仍能继续使用读状态机。
///
/// 取消由 [`FrameReader`] 内部持有的令牌在每次 `read_async` 上生效，因此这里不再
/// 对传入的令牌做额外处理，只负责补齐 `may_cancel_with` 接口。
#[gen_may_cancel_future(NextEntry, pub(crate))]
async fn next_entry_async_<'s, 'f, 'r, R, K, C>(
    reader: &'r mut FrameReader<'f, R, K>,
    _cancel: C,
) -> Result<NegotiationEntry<'s>, StreamEnd_>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    K: TrCancellationToken + 'f,
    C: TrCancellationToken,
{
    let res: Result<Option<NegotiationEntry<'s>>, WireError<R::Err, ()>> =
        FrameReader::next_entry_async_(&mut *reader).await;
    match res {
        // 有下一条：正常产出。
        Result::Ok(Option::Some(entry)) => Result::Ok(entry),
        // 条目读完（正常结束）：[`TrAsyncIterator`] 用 `Err` 表达「不能再产出」。
        Result::Ok(Option::None) => Result::Err(StreamEnd_::Ended),
        Result::Err(err) => {
            // 详细原因留给调用方取回；流本身只需报告「终止」。
            reader.last_err_ = Option::Some(err);
            Result::Err(StreamEnd_::Failed)
        }
    }
}

/// 握手帧读取状态机。
///
/// 生命周期 `'f` 是底层读缓冲的借用；[`FrameReader`] 本身**不累积整帧**：
///
/// - 基础项解码后存进 5 个定长槽位（[`NegotiationBasicEntry`]），原始字节丢弃；
/// - 扩展条目在 v1 中是保留键，读到即 `UnsupportedOption`（见 `dev-notes.md`）；
/// - 校验只维护一个 [`CrcDigest`]。
///
/// 因此内存占用是 O(1)（相对帧长与条目数量），条目数量不设上限也不会撑爆内存。
pub(crate) struct FrameReader<'f, R: TrBuffRead<u8>, K> {
    cursor_: ReadCursor<'f, R>,

    /// 读侧自己的取消令牌（`K` 的克隆）。持有克隆而不是借用，读状态机才能与
    /// 「协商时把真正的令牌借给协商器」并存而不冲突。
    cancel_: K,

    crc_: CrcDigest,

    /// 算法预告 / 校验头的原始字节，两处必须逐位一致。
    alg_hdr_: u8,

    /// 校验码的字节数（2/3/4），由算法预告决定；与 `alg_hdr_` 一同取代原先
    /// 整个 `HandshakeChecksum` 值，避免在 `FrameReader` 里放一个约 1 KB 的字段。
    checksum_len_: usize,

    magic_: MagicField,

    basics_: [Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT],

    /// 已出现的基础键位图，用于重复键检测。
    seen_: u8,

    /// 是否已经读到校验头并完成 `crc` 比对。
    done_: bool,

    /// 条目流终止时的详细失败原因，供调用方在协商结束后取回并分类处置。
    last_err_: Option<WireError<R::Err, ()>>,
}

impl<'f, R, K> FrameReader<'f, R, K>
where
    R: TrBuffRead<u8>,
    K: TrCancellationToken,
{
    /// 读取 `magic` 与算法预告，建立一个帧读取状态机。
    ///
    /// `magic` 的语义（`INVITE` / `ACCEPT` / …）由调用方通过
    /// [`FrameReader::magic_`] 判定；这里只保证读到 4 字节。
    ///
    /// # Errors
    ///
    /// 底层读失败、对端关闭，或算法预告不是 `0x1C` / `0x2C` / `0x3C` 之一时
    /// 返回错误。算法预告非法必须立即终止——此时既无法确定 `crc` 长度，也无法
    /// 定位帧尾（模块文档 §4.1）。
    pub(crate) async fn begin_async_(
        buff: &'f mut R,
        cancel: K,
    ) -> Result<Self, WireError<R::Err, ()>> {
        let mut cursor = ReadCursor::new_(buff);
        let mut magic = [0u8; 4];
        cursor.read_async_(&mut magic, cancel.child_token()).await?;

        let mut alg = [0u8; 1];
        cursor.read_async_(&mut alg, cancel.child_token()).await?;
        let alg_hdr = alg[0];
        let Option::Some(checksum) = HandshakeChecksum::try_header(alg_hdr) else {
            return Result::Err(WireError::UnsupportedOption);
        };

        let mut crc = CrcDigest::new_(&checksum);
        // 校验覆盖 magic 与算法预告本身（模块文档 §4）。
        crc.update_(&magic);
        crc.update_(&alg);

        Result::Ok(FrameReader {
            cursor_: cursor,
            cancel_: cancel,
            crc_: crc,
            alg_hdr_: alg_hdr,
            checksum_len_: checksum.checksum_len(),
            magic_: magic,
            basics_: core::array::from_fn(|_| Option::None),
            seen_: 0u8,
            done_: false,
            last_err_: Option::None,
        })
    }

    /// 帧首 4 字节 `magic`。
    pub(crate) fn magic_(&self) -> MagicField {
        self.magic_
    }

    /// 已解析的基础项槽位；下标即基础键 `0x00..=0x04`。
    pub(crate) fn basics_(&self) -> &[Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT] {
        &self.basics_
    }

    /// 是否已经读到校验头并完成 `crc` 比对。
    pub(crate) fn is_finished_(&self) -> bool {
        self.done_
    }

    /// 取回条目流终止的详细原因（若有）。
    ///
    /// 协商器只拿到 [`StreamEnd_`]，无法把底层读错误 / 校验失败带出来；因此
    /// 读状态机把它记在这里，由调用方在协商返回后取回。
    pub(crate) fn take_error_(&mut self) -> Option<WireError<R::Err, ()>> {
        self.last_err_.take()
    }

    /// 读出**下一个条目**；读到校验头则校验 `crc` 并返回 `None`。
    ///
    /// 这是流式协商的核心：每读到一条就立刻返回给调用方，调用方可以马上判断
    /// 接受还是拒绝，**不必等整帧**（模块文档 §11）。
    ///
    /// # Errors
    ///
    /// - 未知 / 保留键、非法 `val_type`、非法校验头取值 → `UnsupportedOption`；
    /// - 重复键、基础项取值为 0、数值超出 `usize`、校验头与算法预告不一致
    ///   → `MalformedBody`；
    /// - `crc` 不匹配 → `ChecksumErr`；
    /// - 底层读失败 / 对端关闭 → `Read` / `PeerClosed`。
    pub(crate) async fn next_entry_async_(
        &mut self,
    ) -> Result<Option<NegotiationEntry<'f>>, WireError<R::Err, ()>> {
        if self.done_ {
            return Result::Ok(Option::None);
        }

        let mut header = [0u8; 1];
        self.cursor_
            .read_async_(&mut header, self.cancel_.child_token())
            .await?;
        self.crc_.update_(&header);
        let byte = header[0];

        let Ok(key) = NegotiationKey::try_from(byte) else {
            return Result::Err(WireError::UnsupportedOption);
        };

        match key {
            NegotiationKey::Checksum => {
                // 校验头：先确认取值合法，再确认与算法预告一致。
                let Option::Some(_) = HandshakeChecksum::try_header(byte) else {
                    return Result::Err(WireError::UnsupportedOption);
                };
                if byte != self.alg_hdr_ {
                    return Result::Err(WireError::MalformedBody);
                }
                let width = self.checksum_len_;
                let mut bytes = [0u8; 4];
                self.cursor_
                    .read_async_(&mut bytes[..width], self.cancel_.child_token())
                    .await?;
                let expect = decode_checksum_(width, &bytes[..width]);
                // 校验一票否决：算出的值与帧尾声明的值不等即整帧失败。
                if self.crc_.finalize_() != expect {
                    return Result::Err(WireError::ChecksumErr);
                }
                self.done_ = true;
                Result::Ok(Option::None)
            }

            // 扩展条目在 v1 中是保留键（模块文档 §6.3）；即便将来启用，也只会
            // 使用「先校验声明长度、再准备等长内部缓冲」的路径，不会预分配整帧。
            NegotiationKey::ExtMsg => Result::Err(WireError::UnsupportedOption),

            basic => {
                let Ok(val_type) = NegotiationValType::try_from(byte) else {
                    return Result::Err(WireError::UnsupportedOption);
                };
                let Ok(idx) = basic_key_(basic) else {
                    return Result::Err(WireError::UnsupportedOption);
                };

                // 单条目定长栈缓冲：基础项最多 8 字节（BeU64），与帧长无关。
                let width = val_type.value_len();
                let mut bytes = [0u8; 8];
                self.cursor_
                    .read_async_(&mut bytes[..width], self.cancel_.child_token())
                    .await?;
                self.crc_.update_(&bytes[..width]);

                let value =
                    decode_value_(width, &bytes[..width]).map_err(|_| WireError::MalformedBody)?;
                if value == 0 {
                    return Result::Err(WireError::MalformedBody);
                }
                let bit = 1u8 << idx;
                if self.seen_ & bit != 0 {
                    return Result::Err(WireError::MalformedBody);
                }
                self.seen_ |= bit;

                let entry = NegotiationBasicEntry {
                    opts_key: byte,
                    val_data: value,
                };
                self.basics_[idx as usize] = Option::Some(entry.clone());
                Result::Ok(Option::Some(NegotiationEntry::Basic(entry)))
            }
        }
    }

    /// 读完剩余条目并校验 `crc`，丢弃条目内容。
    ///
    /// 协商器提前接受、没有把条目区读到底时，由调用方用它补完校验（见
    /// `dev-notes.md` D1：校验一票否决）。
    pub(crate) async fn drain_async_(&mut self) -> Result<(), WireError<R::Err, ()>> {
        while self.next_entry_async_().await?.is_some() {}
        Result::Ok(())
    }
}

impl<'s, 'f, R, K> TrAsyncIterator for &'s mut FrameReader<'f, R, K>
where
    R: TrBuffRead<u8> + 'f,
    K: TrCancellationToken + 'f,
{
    // 条目生命周期取**外层借用** `'s` 而不是读缓冲的 `'f`：`NegotiationEntry`
    // 对 `'f` 协变，而 `'f: 's`，因此可以把 `'f` 的条目收窄成 `'s`。这样协商器
    // 的借用与条目流借用同一个（较短的）生命周期，调用方在协商返回后仍能继续
    // 使用读状态机。条目流的 step future 由 `gen_may_cancel_future` 生成的
    // `NextEntryAsync` 充当，不再需要手写的适配层。
    type Item = NegotiationEntry<'s>;
    type Err = StreamEnd_;

    type NextAsync<'g> = NextEntryAsync<'s, 'f, 'g, 'g, R, K>
    where
        Self: 'g;

    fn next_async(&mut self) -> Self::NextAsync<'_> {
        NextEntryAsync::new(&mut **self)
    }
}

/// 条目流的终止标记。
///
/// [`TrAsyncIterator`] 的契约是 `Result<Item, Err>`——**没有** `Option`，因此「条目
/// 读完」也要用 `Err` 表达。协商器只需要知道「流已经不能再继续」，不必区分两种终止；
/// 真正的失败原因由 [`FrameReader::take_error_`] 取回，调用方据此按模块文档 §9 分类
/// 处置——例如 `ChecksumErr` 与主动拒绝的处置完全不同（前者不得回 `REJECT`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StreamEnd_ {
    /// 条目流已经读到底（正常结束）。
    #[error("握手条目流已终止")]
    Ended,

    /// 读侧发生失败，条目流就此终止。
    #[error("握手条目流已终止")]
    Failed,
}

/// 握手帧编解码失败。
///
/// 读写两侧的底层错误都原样携带，由
/// [`HandshakeError`](super::error::HandshakeError) 统一呈现给调用方。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WireError<RE, WE> {
    /// 底层读错误。
    #[error("握手帧读取失败")]
    Read(RE),

    /// 底层写错误。
    #[error("握手帧写入失败")]
    Write(WE),

    /// 未知 / 保留键，或算法预告 / 校验头的 `val_type` 不是 16/24/32 位校验码。
    #[error("握手帧包含不支持的键或校验类型")]
    UnsupportedOption,

    /// 条目区结构非法：重复键、基础项取值为 0、数值超出 `usize`、
    /// 校验头与算法预告不一致。
    #[error("握手帧条目区结构非法")]
    MalformedBody,

    /// CRC 不匹配。
    #[error("握手帧校验失败")]
    ChecksumErr,

    /// 帧未读完对端就已经关闭。
    #[error("对端在握手帧中途关闭连接")]
    PeerClosed,
}

/// 把共享字节游标（[`crate::wire_io_`]）的错误映射为握手帧错误。
///
/// 游标只报告「底层怎么失败的」，握手侧在这里把它翻译成自己的语义，因此
/// [`FrameReader`] 与 [`write_frame_`] 的调用点可以继续直接使用 `?`。
impl<RE, WE> From<CursorError<RE, WE>> for WireError<RE, WE> {
    fn from(err: CursorError<RE, WE>) -> Self {
        match err {
            CursorError::Read(err) => WireError::Read(err),
            CursorError::Write(err) => WireError::Write(err),
            CursorError::PeerClosed => WireError::PeerClosed,
        }
    }
}

/// 逐字段写出一个握手帧。
///
/// **不预先成形整帧**：`magic`、算法预告、各条目、校验头、`crc` 依次写出，
/// 每写一段就把它计入 [`CrcDigest`]，因此写侧内存同样是 O(1)。`values[k]` 为
/// `None` 时跳过键 `k`；每个条目使用能容纳该值的最小 `val_type` 宽度
/// （模块文档 §6）。
///
/// `checksum` 指定本帧使用的校验算法，帧会在自己的算法预告与校验头两个字节中
/// 声明同一个取值。
///
/// # Errors
///
/// 底层写入失败或对端提前关闭时返回 [`WireError::Write`]。
pub(super) async fn write_frame_<W, K>(
    buff: &mut W,
    magic: MagicField,
    values: &[Option<usize>; K_BASIC_KEY_COUNT],
    checksum: &HandshakeChecksum,
    cancel: K,
) -> Result<(), WireError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    let mut crc = CrcDigest::new_(checksum);

    // 1. magic。
    write_all_async_(buff, &magic, cancel.child_token()).await?;
    crc.update_(&magic);

    // 2. 算法预告：与帧尾校验头逐位相同。
    let alg_hdr = checksum.header_byte_();
    let alg = [alg_hdr];
    write_all_async_(buff, &alg, cancel.child_token()).await?;
    crc.update_(&alg);

    // 3. 条目区：逐条成形（最大 9 字节栈缓冲）后立即写出。
    for (key, value) in values.iter().enumerate() {
        let Option::Some(value) = value else {
            continue;
        };
        let Ok(key) = NegotiationKey::try_from(key as u8) else {
            return Result::Err(WireError::UnsupportedOption);
        };
        let (vl_type, entry) = encode_entry_(key, *value);
        let width = 1usize + vl_type.value_len();
        write_all_async_(buff, &entry[..width], cancel.child_token()).await?;
        crc.update_(&entry[..width]);
    }

    // 4. 校验头（定界符）。
    write_all_async_(buff, &alg, cancel.child_token()).await?;
    crc.update_(&alg);

    // 5. crc 值。
    let (bytes, len) = checksum_bytes_(checksum, crc.finalize_());
    write_all_async_(buff, &bytes[..len], cancel.child_token()).await?;
    Result::Ok(())
}

/// 组装条目头字节：`header = (val_type << 4) | key`。
const fn compose_header_(key: NegotiationKey, vl_type: NegotiationValType) -> u8 {
    (key as u8) | (vl_type as u8)
}

/// 把基础键映射为下标；`Checksum` 与保留键不在这里处理。
fn basic_key_(key: NegotiationKey) -> Result<u8, NegotiationKey> {
    match key {
        NegotiationKey::MaxPacketSize => Result::Ok(0u8),
        NegotiationKey::MaxChannelCount => Result::Ok(1u8),
        NegotiationKey::MaxDockChanCount => Result::Ok(2u8),
        NegotiationKey::MaxChannelTimeout => Result::Ok(3u8),
        NegotiationKey::MaxChannelWaitClose => Result::Ok(4u8),
        other => Result::Err(other),
    }
}

/// 把校验码的数值写成 `checksum_len()` 字节大端序。
fn checksum_bytes_(checksum: &HandshakeChecksum, value: u32) -> ([u8; 4], usize) {
    let all = value.to_be_bytes();
    let len = checksum.checksum_len();
    let mut out = [0u8; 4];
    out[..len].copy_from_slice(&all[4 - len..]);
    (out, len)
}

/// 把大端 `width` 字节的校验码解码为数值。
///
/// CRC-24 只有 3 字节，这里统一按 `u32` 承载。
fn decode_checksum_(width: usize, bytes: &[u8]) -> u32 {
    debug_assert_eq!(width, bytes.len());
    let mut value = 0u32;
    for &b in bytes {
        value = (value << 8) | b as u32;
    }
    value
}

/// 把 `width` 字节大端无符号数解码为 `usize`；超出 `usize` 表示范围返回错误。
fn decode_value_(width: usize, bytes: &[u8]) -> Result<usize, WireError<(), ()>> {
    debug_assert_eq!(width, bytes.len());
    let mut value = 0u64;
    for &b in bytes {
        value = (value << 8) | b as u64;
    }
    usize::try_from(value).map_err(|_| WireError::MalformedBody)
}

/// 编码一个条目：`header` 字节 + 大端 `value`，使用能容纳该值的最小宽度。
///
/// 返回 `(val_type, 整个条目的字节)`；有效长度为
/// `1 + val_type.value_len()`。
fn encode_entry_(key: NegotiationKey, value: usize) -> (NegotiationValType, [u8; 9]) {
    let vl_type = crate::handshake::opts::min_val_type_(value);
    let width = vl_type.value_len();
    let mut out = [0u8; 9];
    out[0] = key | vl_type;
    let all = (value as u64).to_be_bytes();
    out[1..1 + width].copy_from_slice(&all[8 - width..]);
    (vl_type, out)
}

/// 把基础键的可选取值集合补全为 [`BasicOpts`]；缺位项使用协议缺省值。
pub(super) fn values_to_basic_(values: &[Option<usize>; K_BASIC_KEY_COUNT]) -> BasicOpts {
    let default_wait_close = BasicOpts::DEFAULT.max_channel_wait_close.as_secs() as usize;
    BasicOpts {
        max_packet_size: values[0].unwrap_or(BasicOpts::DEFAULT.max_packet_size),
        max_channel_count: values[1].unwrap_or(BasicOpts::DEFAULT.max_channel_count),
        max_dock_chan_count: values[2].unwrap_or(BasicOpts::DEFAULT.max_dock_chan_count),
        max_channel_timeout: Duration::from_secs(values[3].unwrap_or(30usize) as u64),
        max_channel_wait_close: Duration::from_secs(values[4].unwrap_or(default_wait_close) as u64),
    }
}

/// 把 [`BasicOpts`] 展开为 5 个全部存在的基础键取值。
pub(super) fn basic_to_values_(opts: &BasicOpts) -> [Option<usize>; K_BASIC_KEY_COUNT] {
    [
        Option::Some(opts.max_packet_size),
        Option::Some(opts.max_channel_count),
        Option::Some(opts.max_dock_chan_count),
        Option::Some(opts.max_channel_timeout.as_secs() as usize),
        Option::Some(opts.max_channel_wait_close.as_secs() as usize),
    ]
}

/// 判断 5 个基础键是否全部出现。
pub(super) fn is_complete_(values: &[Option<usize>; K_BASIC_KEY_COUNT]) -> bool {
    values.iter().all(Option::is_some)
}

/// 等待方补全规则（模块文档 §7.2）：以 `local` 为基础，用 `INVITE` 中已提及的
/// 项覆盖对应位置。
pub(super) fn complete_invite_(
    local: &BasicOpts,
    entries: &[Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT],
) -> [Option<usize>; K_BASIC_KEY_COUNT] {
    let mut values = basic_to_values_(local);
    for (k, entry) in entries.iter().enumerate() {
        if let Option::Some(entry) = entry {
            values[k] = Option::Some(entry.val_data);
        }
    }
    values
}

/// 把流式读出的条目转换为按键下标排列的取值；`ACCEPT` / `CONFIRM` 必须补齐
/// 全部 5 项，否则返回 `None`。
pub(super) fn complete_values_(
    entries: &[Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT],
) -> Option<[Option<usize>; K_BASIC_KEY_COUNT]> {
    let mut values: [Option<usize>; K_BASIC_KEY_COUNT] = [Option::None; K_BASIC_KEY_COUNT];
    for (k, entry) in entries.iter().enumerate() {
        values[k] = entry.as_ref().map(|e| e.val_data);
    }
    if is_complete_(&values) {
        Option::Some(values)
    } else {
        Option::None
    }
}

/// 校验 `CONFIRM` 的条目是否恰好等于 `expected`（模块文档 §7.4）。
pub(super) fn confirm_matches_(
    entries: &[Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT],
    expected: &[Option<usize>; K_BASIC_KEY_COUNT],
) -> bool {
    let Option::Some(values) = complete_values_(entries) else {
        return false;
    };
    values.iter().zip(expected.iter()).all(|(a, b)| a == b)
}

#[cfg(test)]
mod tests_ {
    use buffex::x_deps::abs_cancel::NonCancellableToken;

    use super::*;
    use crate::handshake::{K_ACCEPT_MAGIC, K_INVITE_MAGIC};

    /// 把一帧写进 `buf`，返回写入的字节数。
    async fn write_into_buf_(
        buf: &mut [u8],
        magic: MagicField,
        values: &[Option<usize>; K_BASIC_KEY_COUNT],
        checksum: &HandshakeChecksum,
    ) -> usize {
        let capacity = buf.len();
        let mut cursor: &mut [u8] = buf;
        let cancel = NonCancellableToken::new();
        write_frame_(&mut cursor, magic, values, checksum, cancel)
            .await
            .expect("写帧应当成功");
        capacity - cursor.len()
    }

    /// 测试「帧首算法预告 + 单趟增量校验」的正确性。
    /// - 手段：三种算法下，用 `write_frame_` 写帧后交给 `FrameReader` 逐条读完，
    ///   并把 `CrcDigest` 的结果与 `crc::Crc::checksum` 的一次性结果对比。
    /// - 判断：`FrameReader` 读完后 `is_finished_()` 为真，且增量校验值与
    ///   `crc` crate 的结果完全一致。
    async fn incremental_crc_matches_one_shot() {
        let values = [
            Option::Some(7usize),
            Option::None,
            Option::None,
            Option::None,
            Option::None,
        ];
        let cases = [
            HandshakeChecksum::Crc16(&HANDSHAKE_CRC16),
            HandshakeChecksum::Crc24(&HANDSHAKE_CRC24),
            HandshakeChecksum::Crc32(&HANDSHAKE_CRC32),
        ];
        for checksum in cases {
            let mut buf = [0u8; 64];
            let total = write_into_buf_(&mut buf, K_INVITE_MAGIC, &values, &checksum).await;

            // 校验覆盖区 = 除末尾 crc 之外的全部字节。
            let coverage_len = total - checksum.checksum_len();
            let one_shot = match &checksum {
                HandshakeChecksum::Crc16(_) => {
                    HANDSHAKE_CRC16.checksum(&buf[..coverage_len]) as u32
                }
                HandshakeChecksum::Crc24(_) => HANDSHAKE_CRC24.checksum(&buf[..coverage_len]),
                HandshakeChecksum::Crc32(_) => HANDSHAKE_CRC32.checksum(&buf[..coverage_len]),
            };
            let mut accum = CrcDigest::new_(&checksum);
            accum.update_(&buf[..coverage_len]);
            assert_eq!(accum.finalize_(), one_shot);

            let mut probe: &[u8] = &buf[..total];
            let mut reader =
                FrameReader::begin_async_(&mut probe, NonCancellableToken::new())
                    .await
                    .expect("magic 与算法预告应当可读");
            assert_eq!(reader.magic_(), K_INVITE_MAGIC);
            reader.drain_async_().await.expect("整帧应当校验通过");
            assert!(reader.is_finished_());
            assert_eq!(reader.basics_()[0].as_ref().unwrap().val_data, 7usize);
        }
    }
    dual_runtime_test_!(incremental_crc_matches_one_shot);

    /// 测试三种校验算法都能被写侧声明、被读侧识别。
    /// - 手段：分别用 CRC-16 / CRC-24 / CRC-32 写帧，再用 `FrameReader` 读完。
    /// - 判断：三次读取都成功，且帧长符合 `4 + 1 + 2 + 1 + crc_len`。
    async fn roundtrip_supports_crc16_crc24_crc32() {
        let values = [
            Option::Some(7usize),
            Option::None,
            Option::None,
            Option::None,
            Option::None,
        ];
        let cases = [
            (
                HandshakeChecksum::Crc16(&HANDSHAKE_CRC16),
                NegotiationValType::BeU16,
                2usize,
            ),
            (
                HandshakeChecksum::Crc24(&HANDSHAKE_CRC24),
                NegotiationValType::BeU24,
                3usize,
            ),
            (
                HandshakeChecksum::Crc32(&HANDSHAKE_CRC32),
                NegotiationValType::BeU32,
                4usize,
            ),
        ];
        for (checksum, vl_type, crc_len) in cases {
            assert_eq!(u8::from(checksum.header()), u8::from(vl_type));
            assert_eq!(checksum.checksum_len(), crc_len);
            let mut buf = [0u8; 64];
            let total = write_into_buf_(&mut buf, K_ACCEPT_MAGIC, &values, &checksum).await;
            let mut probe: &[u8] = &buf[..total];
            let mut reader =
                FrameReader::begin_async_(&mut probe, NonCancellableToken::new())
                    .await
                    .expect("magic 与算法预告应当可读");
            reader.drain_async_().await.expect("整帧应当校验通过");
            assert_eq!(reader.basics_()[0].as_ref().unwrap().val_data, 7usize);
            // magic(4) + 算法预告(1) + 条目(1 + 1) + 校验头(1) + crc。
            assert_eq!(total, 4 + 1 + 2 + 1 + crc_len);
        }
    }
    dual_runtime_test_!(roundtrip_supports_crc16_crc24_crc32);

    /// 测试 CRC-24 校验码被篡改时会被识别。
    /// - 手段：用 CRC-24 写帧后翻转校验码最后一个字节。
    /// - 判断：读完帧时返回 `ChecksumErr`。
    async fn crc24_mismatch_is_rejected() {
        let values = [
            Option::Some(7usize),
            Option::None,
            Option::None,
            Option::None,
            Option::None,
        ];
        let checksum = HandshakeChecksum::Crc24(&HANDSHAKE_CRC24);
        let mut buf = [0u8; 64];
        let total = write_into_buf_(&mut buf, K_INVITE_MAGIC, &values, &checksum).await;
        buf[total - 1] ^= 0xFF;
        let mut probe: &[u8] = &buf[..total];
        let mut reader = FrameReader::begin_async_(&mut probe, NonCancellableToken::new())
            .await
            .expect("magic 与算法预告应当可读");
        let res = reader.drain_async_().await;
        assert!(matches!(res, Result::Err(WireError::ChecksumErr)));
    }
    dual_runtime_test_!(crc24_mismatch_is_rejected);

    /// 测试 CRC-8（`BeU8`）不是合法算法预告。
    /// - 手段：手工构造 `magic + 0x0C`（`BeU8` + Checksum）。
    /// - 判断：`FrameReader::begin_async_` 返回 `UnsupportedOption`。
    async fn crc8_alg_hint_is_unsupported() {
        let mut buf = [0u8; 8];
        buf[..4].copy_from_slice(&K_INVITE_MAGIC);
        buf[4] = compose_header_(NegotiationKey::Checksum, NegotiationValType::BeU8);
        let mut probe: &[u8] = &buf;
        let res = FrameReader::begin_async_(&mut probe, NonCancellableToken::new()).await;
        assert!(matches!(res, Result::Err(WireError::UnsupportedOption)));
    }
    dual_runtime_test_!(crc8_alg_hint_is_unsupported);

    /// 测试校验头与算法预告不一致会被拒绝。
    /// - 手段：算法预告用 CRC-16，帧尾校验头改成 CRC-32。
    /// - 判断：读到校验头时返回 `MalformedBody`。
    async fn checksum_header_mismatch_is_rejected() {
        let values = [
            Option::Some(7usize),
            Option::None,
            Option::None,
            Option::None,
            Option::None,
        ];
        let mut buf = [0u8; 64];
        let total = write_into_buf_(
            &mut buf,
            K_INVITE_MAGIC,
            &values,
            &HandshakeChecksum::Crc16(&HANDSHAKE_CRC16),
        )
        .await;
        // 帧尾校验头是倒数第 3 个字节（2 字节 crc）。
        let trailer = total - 3;
        buf[trailer] = compose_header_(NegotiationKey::Checksum, NegotiationValType::BeU32);
        let mut probe: &[u8] = &buf[..total];
        let mut reader = FrameReader::begin_async_(&mut probe, NonCancellableToken::new())
            .await
            .expect("magic 与算法预告应当可读");
        let res = reader.drain_async_().await;
        assert!(matches!(res, Result::Err(WireError::MalformedBody)));
    }
    dual_runtime_test_!(checksum_header_mismatch_is_rejected);

    /// 测试未知 / 保留键会被拒绝。
    /// - 手段：构造头字节键为 `0x05`（保留段）的条目。
    /// - 判断：读到该条目时返回 `UnsupportedOption`。
    async fn reserved_key_is_rejected() {
        let mut buf = [0u8; 16];
        buf[..4].copy_from_slice(&K_INVITE_MAGIC);
        // 算法预告：CRC-16。
        buf[4] = compose_header_(NegotiationKey::Checksum, NegotiationValType::BeU16);
        // 非法条目：key = 0x05。
        buf[5] = 0x05;
        buf[6] = 0x01;
        let mut probe: &[u8] = &buf[..7];
        let mut reader = FrameReader::begin_async_(&mut probe, NonCancellableToken::new())
            .await
            .expect("magic 与算法预告应当可读");
        let res = reader.drain_async_().await;
        assert!(matches!(res, Result::Err(WireError::UnsupportedOption)));
    }
    dual_runtime_test_!(reserved_key_is_rejected);

    /// 测试扩展条目（`0x0E`）在 v1 中按保留键处理。
    /// - 手段：构造头字节键为 `0x0E` 的条目。
    /// - 判断：读到该条目时返回 `UnsupportedOption`。
    async fn ext_key_is_rejected_in_v1() {
        let mut buf = [0u8; 16];
        buf[..4].copy_from_slice(&K_INVITE_MAGIC);
        buf[4] = compose_header_(NegotiationKey::Checksum, NegotiationValType::BeU16);
        buf[5] = compose_header_(NegotiationKey::ExtMsg, NegotiationValType::BeU8);
        buf[6] = 0x00;
        let mut probe: &[u8] = &buf[..7];
        let mut reader = FrameReader::begin_async_(&mut probe, NonCancellableToken::new())
            .await
            .expect("magic 与算法预告应当可读");
        let res = reader.drain_async_().await;
        assert!(matches!(res, Result::Err(WireError::UnsupportedOption)));
    }
    dual_runtime_test_!(ext_key_is_rejected_in_v1);

    /// 测试重复键会被拒绝。
    /// - 手段：构造同一基础键 `0x00` 出现两次的条目区，其后补合法 CRC-16 校验尾。
    /// - 判断：读到第二个条目时返回 `MalformedBody`。
    async fn duplicate_key_is_rejected() {
        let mut buf = [0u8; 16];
        buf[..4].copy_from_slice(&K_INVITE_MAGIC);
        buf[4] = compose_header_(NegotiationKey::Checksum, NegotiationValType::BeU16);
        buf[5] = 0x00;
        buf[6] = 0x01;
        buf[7] = 0x00;
        buf[8] = 0x02;
        buf[9] = compose_header_(NegotiationKey::Checksum, NegotiationValType::BeU16);
        let crc = HANDSHAKE_CRC16.checksum(&buf[..10]);
        buf[10..12].copy_from_slice(&crc.to_be_bytes());
        let mut probe: &[u8] = &buf[..12];
        let mut reader = FrameReader::begin_async_(&mut probe, NonCancellableToken::new())
            .await
            .expect("magic 与算法预告应当可读");
        let res = reader.drain_async_().await;
        assert!(matches!(res, Result::Err(WireError::MalformedBody)));
    }
    dual_runtime_test_!(duplicate_key_is_rejected);

    /// 测试基础项取值为 0 会被拒绝。
    /// - 手段：构造 `key = 0x00, value = 0x00` 的条目。
    /// - 判断：返回 `MalformedBody`。
    async fn zero_value_is_rejected() {
        let mut buf = [0u8; 16];
        buf[..4].copy_from_slice(&K_INVITE_MAGIC);
        buf[4] = compose_header_(NegotiationKey::Checksum, NegotiationValType::BeU16);
        buf[5] = 0x00;
        buf[6] = 0x00;
        let mut probe: &[u8] = &buf[..7];
        let mut reader = FrameReader::begin_async_(&mut probe, NonCancellableToken::new())
            .await
            .expect("magic 与算法预告应当可读");
        let res = reader.drain_async_().await;
        assert!(matches!(res, Result::Err(WireError::MalformedBody)));
    }
    dual_runtime_test_!(zero_value_is_rejected);

    /// 测试条目是**逐条**产出的：解析一条只需要该条目自身的字节，不必等整帧。
    /// - 手段：写入一个完整帧，然后**截断**成只剩 `magic + 算法预告 + 第一条`；
    ///   用 `FrameReader` 读第一条，再尝试读下一条。
    /// - 判断：第一条能成功读出（证明不需要整帧）；此时 `is_finished_()` 为假；
    ///   继续读会因为缺少后续字节而失败。若读侧依赖整帧，第一步就会失败。
    async fn entries_are_yielded_incrementally() {
        let values = [
            Option::Some(4096usize),
            Option::Some(1usize << 28),
            Option::Some(64usize),
            Option::Some(30usize),
            Option::Some(5usize),
        ];
        let checksum = HandshakeChecksum::Crc16(&HANDSHAKE_CRC16);
        let mut buf = [0u8; 64];
        let total = write_into_buf_(&mut buf, K_INVITE_MAGIC, &values, &checksum).await;

        // 只保留 magic(4) + 算法预告(1) + 第一条 BeU16 条目(3)。
        let cut = 5usize + 3usize;
        assert!(cut < total);
        let mut probe: &[u8] = &buf[..cut];
        // 用块限制读状态机的借用范围，块结束后才能检查 `probe`。
        {
            let mut reader =
                FrameReader::begin_async_(&mut probe, NonCancellableToken::new())
                    .await
                    .expect("magic 与算法预告应当可读");

            let first = reader.next_entry_async_().await.expect("第一条应当可读");
            let Option::Some(NegotiationEntry::Basic(first)) = first else {
                panic!("第一条应当是基础条目");
            };
            assert_eq!(first.val_data, 4096usize);
            assert!(!reader.is_finished_(), "整帧尚未读完");

            // 后续字节被截断：再读必定失败。
            let res = reader.next_entry_async_().await;
            assert!(res.is_err(), "截断帧的后续读取应当失败");
            assert!(!reader.is_finished_());
        }
        assert_eq!(probe.len(), 0usize, "读第一条只消费它自己的字节");
    }
    dual_runtime_test_!(entries_are_yielded_incrementally);

    /// 测试等待方补全规则：已提及项覆盖本地值，未提及项保留本地值。
    /// - 手段：本地五项齐全，`INVITE` 只提及 `max_packet_size = 8192`。
    /// - 判断：补全结果第 0 项为 8192，其余四项等于本地值。
    ///
    /// 本测试不涉及 async，故使用同步的 `#[test]`（符合目标 3 的豁免条款）。
    #[test]
    fn complete_invite_overrides_only_mentioned() {
        let local = BasicOpts::DEFAULT;
        let mut entries: [Option<NegotiationBasicEntry>; K_BASIC_KEY_COUNT] =
            core::array::from_fn(|_| Option::None);
        entries[0] = Option::Some(NegotiationBasicEntry {
            opts_key: 0x10,
            val_data: 8192,
        });
        let values = complete_invite_(&local, &entries);
        assert_eq!(values[0], Option::Some(8192));
        assert_eq!(values[1], Option::Some(local.max_channel_count));
        assert_eq!(values[2], Option::Some(local.max_dock_chan_count));
        assert_eq!(
            values[3],
            Option::Some(local.max_channel_timeout.as_secs() as usize)
        );
        assert_eq!(
            values[4],
            Option::Some(local.max_channel_wait_close.as_secs() as usize)
        );
    }

    /// 测试新增基础键 `MaxChannelWaitClose`（`0x04`）的编解码往返一致性。
    /// - 手段：构造一个 `max_channel_wait_close` 取非缺省值（7 秒）的
    ///   [`BasicOpts`]，用 `basic_to_values_` 展开为基础项取值、`write_frame_`
    ///   写成 `INVITE` 帧，再由 `FrameReader` 逐条读回，经 `complete_values_` 与
    ///   `values_to_basic_` 还原为 [`BasicOpts`]。
    /// - 判断：第 5 个槽位（下标 4）对应的键确实是 `MaxChannelWaitClose`，还原出的
    ///   `max_channel_wait_close` 恰为 7 秒，且其余基础项与原始值一致。
    async fn max_channel_wait_close_roundtrips_through_codec() {
        let opts = BasicOpts {
            max_channel_wait_close: Duration::from_secs(7u64),
            ..BasicOpts::default()
        };
        let values = basic_to_values_(&opts);
        let mut buf = [0u8; 64];
        let total = write_into_buf_(
            &mut buf,
            K_INVITE_MAGIC,
            &values,
            &HandshakeChecksum::Crc16(&HANDSHAKE_CRC16),
        )
        .await;

        let mut probe: &[u8] = &buf[..total];
        let mut reader = FrameReader::begin_async_(&mut probe, NonCancellableToken::new())
            .await
            .expect("magic 与算法预告应当可读");
        reader.drain_async_().await.expect("整帧应当校验通过");

        let entry = reader.basics_()[4].as_ref().expect("第 5 槽位应当已填充");
        assert!(matches!(
            NegotiationKey::try_from(entry.opts_key),
            Result::Ok(NegotiationKey::MaxChannelWaitClose)
        ));

        let restored =
            values_to_basic_(&complete_values_(reader.basics_()).expect("五项基础项应当齐全"));
        assert_eq!(restored.max_channel_wait_close, Duration::from_secs(7u64));
        assert_eq!(restored.max_packet_size, opts.max_packet_size);
        assert_eq!(restored.max_channel_count, opts.max_channel_count);
        assert_eq!(restored.max_dock_chan_count, opts.max_dock_chan_count);
        assert_eq!(restored.max_channel_timeout, opts.max_channel_timeout);
    }
    dual_runtime_test_!(max_channel_wait_close_roundtrips_through_codec);

    /// 测试新增基础键 `MaxChannelWaitClose`（`0x04`）的最小宽度选择。
    /// - 手段：用 `encode_entry_` 分别以 5 与 300 为取值编码该键的条目，再按
    ///   头字节的高半字节解析回 [`NegotiationValType`]，并用 `decode_value_`
    ///   把大端数值解回。
    /// - 判断：取值 5 必须选 `BeU8`（头字节 `0x04`）；取值 300 必须选 `BeU16`
    ///   （头字节 `0x14`）；两者解回的数值都与原值相同。
    #[test]
    fn max_channel_wait_close_uses_minimal_width() {
        let (vl_type, bytes) = encode_entry_(NegotiationKey::MaxChannelWaitClose, 5usize);
        assert_eq!(u8::from(vl_type), u8::from(NegotiationValType::BeU8));
        assert_eq!(bytes[0], 0x04);
        assert_eq!(
            decode_value_(vl_type.value_len(), &bytes[1..1 + vl_type.value_len()]),
            Result::Ok(5usize)
        );

        let (vl_type, bytes) = encode_entry_(NegotiationKey::MaxChannelWaitClose, 300usize);
        assert_eq!(u8::from(vl_type), u8::from(NegotiationValType::BeU16));
        assert_eq!(bytes[0], 0x14);
        assert_eq!(
            decode_value_(vl_type.value_len(), &bytes[1..1 + vl_type.value_len()]),
            Result::Ok(300usize)
        );
    }
}
