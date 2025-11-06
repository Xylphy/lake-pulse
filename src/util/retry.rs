use std::error::Error;
use std::future::Future;
use std::time::Duration;
use tracing::warn;

/// Static retry function for retrying operations
pub async fn retry_with_max_retries<F, Fut, T, E>(
    max_retries: usize,
    operation_name: &str,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Error + Send + Sync,
{
    let mut last_error = None;

    for attempt in 0..=max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                // Check if error is retryable (connection errors)
                let error_msg = format!("{:?}", e);
                let is_retryable = error_msg.contains("ConnectionReset")
                    || error_msg.contains("BrokenPipe")
                    || error_msg.contains("Interrupted")
                    || error_msg.contains("TimedOut");

                if !is_retryable || attempt == max_retries {
                    return Err(e);
                }

                warn!(
                    "Retryable error in {} (attempt {}/{}): {:?}",
                    operation_name,
                    attempt + 1,
                    max_retries,
                    e
                );

                last_error = Some(e);

                // Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms, ...
                let backoff_ms = 100 * (1 << attempt.min(10));
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }

    Err(last_error.unwrap())
}
