package consumer

// Consumer receives tasks via REST, updates their state to "processing",
// simulates work by sleeping for task.Value milliseconds,
// and finalizes the state to "done". Subject to a configurable rate limit.

// TODO: Implement REST handler, rate limiter, task processing pipeline
