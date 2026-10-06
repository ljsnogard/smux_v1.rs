//! 临时探针：默认（compio）装配下 MuxConnection 是否 Send + Sync。
#[allow(dead_code)]
pub fn probe() {
    fn assert_send_sync<T: Send + Sync>() {}
    // 用测试支持里的配置构造类型（只做类型级断言，不构造值）。
}
