//! listener 的错误类型。
//!
//! 这里只有**致命**失败：listener 本身再也产不出连接（入向接受失败），listener 已经被
//! 关闭，或本次等待被调用方取消。单条连接的握手失败**不在这里**——非 smux 的连入、
//! 协商被拒、CRC 不匹配等等都属于「这条连接不符合握手条件」，由 listener 静默丢弃并继续
//! 接受下一条；把它们报成错误会让调用方把「有人探测端口」当成故障，也会让迭代器误以为
//! 该收摊了。

/// [`SmuxListener`](crate::listening::SmuxListener) 的 `next_async` 的失败。
///
/// 泛型参数 `AE` 是底层接受操作
/// （[`TrInbound::Error`](crate::config::TrInbound::Error)）的错误类型。
///
/// # 为什么没有「配置侧失败」变体
///
/// [`TrListenerConfig`](crate::config::TrListenerConfig) 的两个取用入口都被约定为
/// **不会失败**：握手条件与连接策略是预存的数据，而帧暂存缓冲的供给由
/// [`make_stage_buffs`](crate::config::TrListenerConfig::make_stage_buffs) 当场完成
/// （分配失败在 `smux_v1` 里是 panic 而不是 `Result`）。将来若某个配置真的需要交出错
/// 误，再加变体也不影响现有匹配。
#[derive(Debug, thiserror::Error)]
pub enum SmuxListenerError<AE> {
    /// **本次**等待被取消。
    ///
    /// 取消令牌由调用方在**每一次**等待上自己施加
    /// （[`TrMayCancel::may_cancel_with`](smux_v1::x_deps::abs_cancel::TrMayCancel::may_cancel_with)），
    /// 它只表示这一次 [`next_async`](crate::listening::SmuxListener) 调用被中止：
    /// listener 本身仍然可用，已经排进队列的连接不会丢，下一次调用会立刻把它取出来。
    #[error("本次等待入向连接被取消")]
    Cancelled,

    /// listener 已经关闭（或投递端已经全部消失），不会再有连接。
    ///
    /// 它出现在两种情形：
    ///
    /// - [`SmuxListener::close_async`](crate::listening::SmuxListener::close_async) 已经
    ///   返回——此时队列里残留的连接也已被丢弃；
    /// - 所有投递端都已退出且队列已取空（例如所有接受槽位都因致命错误退出）。
    ///
    /// 与 [`Cancelled`](SmuxListenerError::Cancelled) 的区别是**这次等待并没有被谁取消**：
    /// 是「不会再有东西」这个事实到达了。
    #[error("监听器已关闭，不会再有连接")]
    Closed,

    /// 从监听套接字接受连接失败（例如 `EMFILE`：进程的 fd 用光了）。
    ///
    /// 这是**致命**信号而不是「这条连接不好」：listener 自己已经产不出连接，因此
    /// 必须让调用方看见。调用方可以据此退避后重试，或直接放弃。
    #[error("接受入向连接失败")]
    Accept(AE),
}

impl<AE> Clone for SmuxListenerError<AE>
where
    AE: Clone,
{
    fn clone(&self) -> Self {
        match self {
            SmuxListenerError::Cancelled => SmuxListenerError::Cancelled,
            SmuxListenerError::Closed => SmuxListenerError::Closed,
            SmuxListenerError::Accept(err) => SmuxListenerError::Accept(err.clone()),
        }
    }
}
