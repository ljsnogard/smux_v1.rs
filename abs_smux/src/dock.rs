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
    T: funty::Integral + Clone + Copy
        + Eq + Ord + PartialEq + PartialOrd,
{
    pub const fn unspecified() -> Self {
        Dock(T::ZERO)
    }

    pub const fn wildcard() -> Self {
        Dock(T::MAX)
    }
}

impl TrDock for Dock<u8> {
    fn unspecified() -> Self {
        Dock::unspecified()
    }

    fn wildcard() -> Self {
        Dock::wildcard()
    }
}

impl TrDock for Dock<u16> {
    fn unspecified() -> Self {
        Dock::unspecified()
    }

    fn wildcard() -> Self {
        Dock::wildcard()
    }
}

impl TrDock for Dock<u32> {
    fn unspecified() -> Self {
        Dock::unspecified()
    }

    fn wildcard() -> Self {
        Dock::wildcard()
    }
}

impl TrDock for Dock<u64> {
    fn unspecified() -> Self {
        Dock::unspecified()
    }

    fn wildcard() -> Self {
        Dock::wildcard()
    }
}

impl TrDock for Dock<usize> {
    fn unspecified() -> Self {
        Dock::unspecified()
    }

    fn wildcard() -> Self {
        Dock::wildcard()
    }
}

impl TrDock for Dock<i8> {
    fn unspecified() -> Self {
        Dock::unspecified()
    }

    fn wildcard() -> Self {
        Dock::wildcard()
    }
}

impl TrDock for Dock<i16> {
    fn unspecified() -> Self {
        Dock::unspecified()
    }

    fn wildcard() -> Self {
        Dock::wildcard()
    }
}

impl TrDock for Dock<i32> {
    fn unspecified() -> Self {
        Dock::unspecified()
    }

    fn wildcard() -> Self {
        Dock::wildcard()
    }
}

impl TrDock for Dock<i64> {
    fn unspecified() -> Self {
        Dock::unspecified()
    }

    fn wildcard() -> Self {
        Dock::wildcard()
    }
}

impl TrDock for Dock<isize> {
    fn unspecified() -> Self {
        Dock::unspecified()
    }

    fn wildcard() -> Self {
        Dock::wildcard()
    }
}
