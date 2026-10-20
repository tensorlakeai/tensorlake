use std::time::Duration;
use tensorlake::{ClientBuilder, ReadRetryConfig};

// Compile as an external consumer: an export hidden inside a private module
// can pass every in-crate test while leaving this configuration unusable.
#[test]
fn read_retry_limits_are_publicly_configurable() {
    ClientBuilder::new("https://sandbox.tensorlake.ai")
        .read_retry(ReadRetryConfig {
            max_attempts: 2,
            attempt_timeout: Duration::from_millis(750),
            total_timeout: Duration::from_secs(2),
        })
        .build()
        .unwrap();
}
