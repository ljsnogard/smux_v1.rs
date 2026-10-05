//! 确定性载荷：同一 `(dock, index)` 生成同一段字节。载荷头 4 字节编码发送方身份，
//! 因此校验**不依赖 open / accept 的配对顺序**。

/// 依据 `(dock, index)` 生成确定性载荷。
///
/// 布局：4 字节大端 tag（`dock << 16 | index`）+ 变长模式体；模式体第 `i` 字节为
/// `((tag + i * 7) % 251) as u8`，长度取 `(index * 37) % 2000 + 8`，因此同一 dock
/// 内不同序号的载荷互不相同，也能覆盖「一次写段的容量边界」。
pub fn make_payload_(dock: u32, index: usize) -> Vec<u8> {
    let tag = (dock << 16) | (index as u32 & 0xFFFF);
    let body_len = (index * 37) % 2000 + 8;
    let mut out = Vec::with_capacity(4 + body_len);
    out.extend_from_slice(&tag.to_be_bytes());
    for i in 0..body_len {
        out.push(((tag as usize).wrapping_add(i * 7) % 251) as u8);
    }
    out
}


/// 流控验收载荷的确定性字节：第 `i` 字节为 `((seed + i * 7) % 251) as u8`。
///
/// 与 [`make_payload_`] 同样的动机（同一条子流上不同序号的载荷互不相同、可复算），
/// 长度由调用方给定——流控用例要的是「远大于窗口」的定长载荷。
pub fn make_flow_payload_(seed: u32, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| ((seed as usize + i * 7) % 251) as u8)
        .collect()
}
