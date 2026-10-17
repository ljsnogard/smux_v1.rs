use abs_buff::x_deps::funty;

/// 垛口。和 TCP/IP 中的端口概念，相似，一对垛口可以定义报文的起点和终点。
///
/// Similar to port in TCP/IP, a pair of docks can define the source and the
/// destination of a packet.
pub trait TrDock
where
    Self: Sized + Clone + Eq + Ord + PartialEq + PartialOrd,
{
    /// 非特定垛口，可用于绑定垛口时表示由连接安排一个可用垛口。
    ///
    /// An unspecified dock can be used as binding target to instruct the
    /// the connection to find and bind an available one.
    fn unspecified() -> Self;

    /// 全指垛口，可用于报文目的地，表示任意接收端可接收。但接收端是否会响应由
    /// 接收端自行决定。
    ///
    /// Specifying all docks as the target, meaning all interested receiver can
    /// receive the packet. However, a response from the receiving peer is not
    /// a must.
    fn wildcard() -> Self;

    fn is_unspecified(&self) -> bool {
        *self == Self::unspecified()
    }

    fn is_wildcard(&self) -> bool {
        *self == Self::wildcard()
    }

    fn is_special(&self) -> bool {
        self.is_unspecified() || self.is_wildcard()
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Dock<T>(T)
where
    T: Clone + Copy + Eq + Ord + PartialEq + PartialOrd;

impl<T> Dock<T>
where
    T: Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
{
    pub const fn new(t: T) -> Self {
        Dock(t)
    }

    /// 取回 dock 的内部数值。
    ///
    /// 上层协议需要把 dock 编成字节（例如复用帧的 `LocalDock` / `RemoteDock`
    /// 字段）时，必须能把它还原成数值；反向构造见 [`Dock::new`]。
    ///
    /// # Examples
    ///
    /// ```
    /// use abs_smux::dock::Dock;
    ///
    /// assert_eq!(Dock::new(7u32).value(), 7u32);
    /// assert_eq!(Dock::<u32>::wildcard().value(), u32::MAX);
    /// ```
    pub const fn value(&self) -> T {
        self.0
    }
}

impl<T> Dock<T>
where
    T: funty::Integral + Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
{
    pub const fn unspecified() -> Self {
        Dock(T::ZERO)
    }

    pub const fn wildcard() -> Self {
        Dock(T::MAX)
    }
}

impl<T> From<T> for Dock<T>
where
    T: funty::Integral + Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
{
    fn from(value: T) -> Self {
        Dock(value)
    }
}

/// 为 [`Dock`] 批量生成 [`TrDock`] 实现与无损 `From` 转换实现。
///
/// 每个条目写作 `目标类型 => [源类型, ...]`，展开后产生：
///
/// - 一个 `impl TrDock for Dock<目标类型>`；
/// - 每个列出的源类型一个 `impl From<源类型> for Dock<目标类型>`。
///
/// 转换体直接调用标准库的 `From` 实现（`Dock(<目标类型>::from(value))`），
/// 因此"源类型能否无损转换为目标类型"完全由标准库的 `From` 实现决定：若某个
/// 组合在标准库中不存在，展开后的代码会直接编译失败，而不会悄悄退化成 `as`
/// 截断转换。
///
/// 类型到自身的转换（如 `Dock<u8>: From<u8>`）已由上面的
/// `impl<T> From<T> for Dock<T>` 统一提供，矩阵中无需重复列出。
///
/// # Examples
///
/// ```text
/// impl_dock_! {
///     u32 => [u8, u16],
/// }
/// ```
///
/// 上例会生成 `impl From<u8> for Dock<u32>`、`impl From<u16> for Dock<u32>`
/// 以及 `impl TrDock for Dock<u32>`。
macro_rules! impl_dock_ {
    (
        $(
            $dst:ty => [$($src:ty),* $(,)?]
        ),* $(,)?
    ) => {
        $(
            impl TrDock for Dock<$dst> {
                fn unspecified() -> Self {
                    Dock::unspecified()
                }

                fn wildcard() -> Self {
                    Dock::wildcard()
                }
            }

            $(
                impl From<$src> for Dock<$dst> {
                    fn from(value: $src) -> Self {
                        Dock(<$dst>::from(value))
                    }
                }
            )*
        )*
    };
}

impl_dock_! {
    // 无符号：只列出标准库保证无损的无符号源类型。`usize` 的位宽随目标平台
    // 变化，因此它只接受 `u8` / `u16`，不接受 `u32` / `u64`。
    u8 => [],
    u16 => [u8],
    u32 => [u8, u16],
    u64 => [u8, u16, u32],
    usize => [u8, u16],
    // 有符号：除更窄的有符号类型外，还接受能完整落入其范围的无符号类型
    // （例如 `u8` 可以无损转成 `i16`）。
    i8 => [],
    i16 => [u8, i8],
    i32 => [u8, u16, i8, i16],
    i64 => [u8, u16, u32, i8, i16, i32],
    isize => [u8, i8, i16],
}

#[cfg(test)]
mod tests_ {
    use super::Dock;
    use super::TrDock;

    /// 测试 `Dock<u32>` 接受所有可无损转换到 `u32` 的无符号源类型。
    ///
    /// - 手段：分别用 `u8`、`u16`、`u32` 的值经 `From` 构造 `Dock<u32>`。
    /// - 判断：构造结果的 `value()` 与原始数值完全相等。
    ///
    /// Test that `Dock<u32>` accepts every unsigned source type that can be
    /// converted to `u32` losslessly.
    /// - Means: construct `Dock<u32>` via `From` from `u8`, `u16` and `u32`
    ///   values.
    /// - Criteria: each constructed `value()` equals the original number.
    #[test]
    fn dock_u32_from_narrower_unsigned_() {
        assert_eq!(Dock::<u32>::from(7u8).value(), 7u32);
        assert_eq!(Dock::<u32>::from(7u16).value(), 7u32);
        assert_eq!(Dock::<u32>::from(7u32).value(), 7u32);
    }

    /// 测试 `Dock<i64>` 接受全部更窄的整数源类型（含有符号与无符号）。
    ///
    /// - 手段：用 `u8`、`u16`、`u32`、`i8`、`i16`、`i32`、`i64` 的值经
    ///   `From` 构造 `Dock<i64>`。
    /// - 判断：构造结果的 `value()` 与原始数值完全相等。
    ///
    /// Test that `Dock<i64>` accepts every narrower integer source type
    /// (signed and unsigned).
    /// - Means: construct `Dock<i64>` via `From` from `u8`, `u16`, `u32`,
    ///   `i8`, `i16`, `i32` and `i64` values.
    /// - Criteria: each constructed `value()` equals the original number.
    #[test]
    fn dock_i64_from_every_narrower_integer_() {
        assert_eq!(Dock::<i64>::from(7u8).value(), 7i64);
        assert_eq!(Dock::<i64>::from(7u16).value(), 7i64);
        assert_eq!(Dock::<i64>::from(7u32).value(), 7i64);
        assert_eq!(Dock::<i64>::from(7i8).value(), 7i64);
        assert_eq!(Dock::<i64>::from(7i16).value(), 7i64);
        assert_eq!(Dock::<i64>::from(7i32).value(), 7i64);
        assert_eq!(Dock::<i64>::from(7i64).value(), 7i64);
    }

    /// 测试位宽随平台变化的 `Dock<usize>` / `Dock<isize>` 只接受标准库
    /// 保证无损的源类型，并保留符号与数值。
    ///
    /// - 手段：用 `u8` / `u16` 构造 `Dock<usize>`，用 `u8` / `i8` / `i16`
    ///   构造 `Dock<isize>`（含负值）。
    /// - 判断：构造结果的 `value()` 与原始数值完全相等，负值符号不变。
    ///
    /// Test that the platform-width `Dock<usize>` / `Dock<isize>` accept only
    /// the source types that the standard library guarantees to be lossless,
    /// while preserving sign and value.
    /// - Means: build `Dock<usize>` from `u8` / `u16`, and `Dock<isize>` from
    ///   `u8` / `i8` / `i16` (including negative values).
    /// - Criteria: each constructed `value()` equals the original number, and
    ///   negative signs are preserved.
    #[test]
    fn dock_usize_and_isize_from_smaller_sources_() {
        assert_eq!(Dock::<usize>::from(7u8).value(), 7usize);
        assert_eq!(Dock::<usize>::from(7u16).value(), 7usize);
        assert_eq!(Dock::<isize>::from(7u8).value(), 7isize);
        assert_eq!(Dock::<isize>::from(-7i8).value(), -7isize);
        assert_eq!(Dock::<isize>::from(-7i16).value(), -7isize);
    }

    /// 测试 `Dock<T>` 到自身的 `From` 实现仍然可用（由泛型自反实现提供）。
    ///
    /// - 手段：用 `u8` 经 `From` 构造 `Dock<u8>`。
    /// - 判断：构造结果的 `value()` 与原始数值完全相等。
    ///
    /// Test that the reflexive `From` implementation for `Dock<T>` is still
    /// available (provided by the generic blanket implementation).
    /// - Means: construct `Dock<u8>` via `From` from a `u8` value.
    /// - Criteria: the constructed `value()` equals the original number.
    #[test]
    fn dock_from_self_is_still_available_() {
        assert_eq!(Dock::<u8>::from(7u8).value(), 7u8);
        assert_eq!(Dock::<u16>::from(7u16).value(), 7u16);
    }

    /// 测试宏为各整数类型生成的 `TrDock` 实现行为一致。
    ///
    /// - 手段：对 `u16` 与 `i32` 分别取 `unspecified()`、`wildcard()`，
    ///   并用普通值调用 `is_special()`。
    /// - 判断：非特定垛口的值等于 `0` 且被识别为非特定；全指垛口的值等于
    ///   类型最大值且被识别为全指；普通值既非非特定也非全指。
    ///
    /// Test that the `TrDock` implementations generated by the macro behave
    /// consistently for every integer type.
    /// - Means: take `unspecified()` and `wildcard()` for `u16` and `i32`, and
    ///   call `is_special()` on an ordinary value.
    /// - Criteria: an unspecified dock equals `0` and is recognised as
    ///   unspecified; a wildcard dock equals the type maximum and is
    ///   recognised as wildcard; an ordinary value is neither.
    #[test]
    fn dock_tr_dock_special_values_are_consistent_() {
        assert!(Dock::<u16>::unspecified().is_unspecified());
        assert_eq!(Dock::<u16>::unspecified().value(), 0u16);
        assert!(Dock::<u16>::wildcard().is_wildcard());
        assert_eq!(Dock::<u16>::wildcard().value(), u16::MAX);
        assert!(Dock::<u16>::wildcard().is_special());
        assert!(!Dock::<u16>::new(1).is_special());

        assert!(Dock::<i32>::unspecified().is_unspecified());
        assert_eq!(Dock::<i32>::unspecified().value(), 0i32);
        assert!(Dock::<i32>::wildcard().is_wildcard());
        assert_eq!(Dock::<i32>::wildcard().value(), i32::MAX);
        assert!(!Dock::<i32>::new(1).is_special());
    }
}
