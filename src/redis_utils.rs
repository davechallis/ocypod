//! Miscellaneous Redis utilities and helper functions.

use redis::{aio::ConnectionLike, from_redis_value, FromRedisValue, Pipeline, RedisResult, Value};

/// Helper function for getting nested data structures from Redis pipelines.
///
/// Used for e.g. querying for vectors of tuples from:
/// pipe.hget(key1, [x, y, z])
///     .hget(key2, [x, y, z])
///     .hget(key3, [x, y, z])
///
/// let (a, b, c): Vec<(x_type, y_type, z_type)> = vec_from_redis_pipe(pipe, conn)?;
pub async fn vec_from_redis_pipe<C: ConnectionLike, T: FromRedisValue>(
    conn: &mut C,
    pipe: &Pipeline,
) -> RedisResult<Vec<T>> {
    let values: Vec<Value> = pipe.query_async(conn).await?;
    let mut results = Vec::with_capacity(values.len());
    for v in values {
        results.push(from_redis_value::<T>(&v)?);
    }

    Ok(results)
}

/// Simplifies async Redis transactions.
#[macro_export]
macro_rules! transaction_async {
    ($conn:expr, $keys:expr, $body:expr) => {
        loop {
            redis::cmd("WATCH").arg($keys).query_async($conn).await?;

            if let Some(response) = $body {
                redis::cmd("UNWATCH").query_async($conn).await?;
                break response;
            }
        }
    };
}
