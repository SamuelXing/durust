use std::time::Duration;

/// Rate limit for workflow starts on a queue.
/// At most `limit` workflows may start within any trailing `period` window.
#[derive(Clone, Debug)]
pub struct RateLimiter {
    /// Maximum number of starts permitted within each `period` window.
    pub limit: i64,
    /// Length of the trailing window the `limit` is measured over.
    pub period: Duration,
}

/// A named durable queue.
///
/// Workflows are enqueued with [`start`](crate::DurableEngine::start) or
/// [`start_with`](crate::DurableEngine::start_with) plus `WorkflowOptions::queue`,
/// and claimed by a per-queue
/// dispatcher task started by [`crate::DurableEngine::launch`]. The dispatcher
/// honors, in order: worker concurrency (this process), global concurrency
/// (across all executors, via a DB count), and the rate limiter.
///
/// Built with chained setters, then registered before `launch`:
///
/// ```
/// use durare::{RateLimiter, WorkflowQueue};
/// use std::time::Duration;
///
/// let q = WorkflowQueue::new("emails")
///     .worker_concurrency(4)
///     .rate_limiter(RateLimiter { limit: 50, period: Duration::from_secs(60) });
/// // engine.register_queue(q) before engine.launch()
/// ```
#[derive(Clone, Debug)]
pub struct WorkflowQueue {
    /// Unique queue name workflows are enqueued on.
    pub name: String,
    /// Max workflows this executor runs concurrently from the queue.
    pub worker_concurrency: Option<usize>,
    /// Max workflows running concurrently across all executors.
    pub global_concurrency: Option<i64>,
    /// When `true`, lower `priority` values are dispatched first.
    pub priority_enabled: bool,
    /// Rate limit on workflow starts from this queue, if any.
    pub rate_limit: Option<RateLimiter>,
    /// Max workflows claimed per polling iteration (default 100).
    pub max_tasks_per_iteration: usize,
    /// When `true`, workflows are enqueued under a partition key and each
    /// partition gets its own concurrency / rate-limit budget (see
    /// [`partitioned`](Self::partitioned)).
    pub partitioned: bool,
    /// Starting (and minimum) polling interval (default 1s).
    pub base_polling_interval: Duration,
    /// Ceiling the interval backs off to on dequeue errors (default 120s).
    pub max_polling_interval: Duration,
}

impl WorkflowQueue {
    /// A new queue with the given name and default settings (unbounded
    /// concurrency, no rate limit, 100 tasks/iteration, 1s–120s polling).
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            worker_concurrency: None,
            global_concurrency: None,
            priority_enabled: false,
            rate_limit: None,
            max_tasks_per_iteration: 100,
            partitioned: false,
            base_polling_interval: Duration::from_secs(1),
            max_polling_interval: Duration::from_secs(120),
        }
    }

    /// Cap the workflows this executor runs concurrently from the queue.
    pub fn worker_concurrency(mut self, n: usize) -> Self {
        self.worker_concurrency = Some(n);
        self
    }

    /// Cap the workflows running concurrently across all executors.
    pub fn global_concurrency(mut self, n: i64) -> Self {
        self.global_concurrency = Some(n);
        self
    }

    /// Dispatch in priority order (lower `priority` value first).
    pub fn priority_enabled(mut self) -> Self {
        self.priority_enabled = true;
        self
    }

    /// Rate-limit workflow starts from this queue.
    pub fn rate_limiter(mut self, r: RateLimiter) -> Self {
        self.rate_limit = Some(r);
        self
    }

    /// Set the maximum workflows claimed per polling iteration (default 100).
    pub fn max_tasks_per_iteration(mut self, n: usize) -> Self {
        self.max_tasks_per_iteration = n;
        self
    }

    /// Enable partitioned mode: enqueue workflows under a partition key (via
    /// [`WorkflowOptions::partition_key`](crate::WorkflowOptions)), and the
    /// dispatcher applies this queue's worker/global concurrency and rate limit
    /// independently per partition. A workflow enqueued to a partitioned queue
    /// without a partition key is never dispatched.
    pub fn partitioned(mut self) -> Self {
        self.partitioned = true;
        self
    }

    /// Set the starting (and minimum) polling interval (default 1s).
    pub fn base_polling_interval(mut self, d: Duration) -> Self {
        self.base_polling_interval = d;
        self
    }

    /// Set the ceiling the polling interval backs off to on errors (default 120s).
    pub fn max_polling_interval(mut self, d: Duration) -> Self {
        self.max_polling_interval = d;
        self
    }
}
