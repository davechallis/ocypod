//! Miscellaneous Redis utilities and helper functions.

use redis::{cmd, ToRedisArgs, FromRedisValue, RedisResult, Value, Pipeline, ConnectionLike, from_redis_value};
use crate::models::OcyResult;

/// Helper function for getting nested data structures from Redis pipelines.
///
/// Used for e.g. querying for vectors of tuples from:
/// pipe.hget(key1, [x, y, z])
///     .hget(key2, [x, y, z])
///     .hget(key3, [x, y, z])
///
/// let (a, b, c): Vec<(x_type, y_type, z_type)> = vec_from_redis_pipe(pipe, conn)?;
pub fn vec_from_redis_pipe<T: FromRedisValue>(
    pipe: &Pipeline,
    conn: &ConnectionLike
) -> RedisResult<Vec<T>> {
    let values: Vec<Value> = pipe.query(conn)?;
    let mut results = Vec::with_capacity(values.len());
    for v in values {
        results.push(from_redis_value::<T>(&v)?);
    }

    Ok(results)
}

/// Helper function to perform transactions in Redis. Based on the `redis::transaction` implementation,
/// but doesn't restrict the return type with `FromRedisValue`.
///
/// Takes a function that returns an `Option<T>`. A return value of `Some(T)` means that the transaction
/// succeeded, and that the loop should be terminated. A value of `None` means that a watched key was
/// modified during the transaction, and that the transaction should be retried.
use std::fmt::Debug;
pub fn transaction<
    K: ToRedisArgs + Debug,
    T,
    F: FnMut(&mut Pipeline) -> OcyResult<Option<T>>
>(
    conn: &ConnectionLike,
    keys: &[K],
    func: F,
) -> OcyResult<T> {
    let mut func = func;
    loop {
        let _: () = cmd("WATCH").arg(keys).query(conn)?;
        let mut p = redis::pipe();
        if let Some(result) = func(p.atomic())? {
            // ensure no watch is left in connection, regardless of whether pipeline was used
            let _: () = cmd("UNWATCH").query(conn)?;
            return Ok(result);
        }
    }
}
