/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  Galactic Message Relay — State Store (Redis)               ║
 * ║  Provides:                                                  ║
 * ║    • Idempotency checks via atomic SETNX                    ║
 * ║    • Retry count tracking via INCR                          ║
 * ╚══════════════════════════════════════════════════════════════╝
 */

const Redis = require("ioredis");

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";
const PROCESSED_PREFIX = "galactic:processed:";
const RETRY_PREFIX = "galactic:retries:";

let redis = null;

/**
 * Connect to Redis with automatic reconnect.
 */
async function connectStateStore(retries = 15, delayMs = 2000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      console.log(`[state] Connecting to Redis (attempt ${attempt}/${retries})…`);
      redis = new Redis(REDIS_URL, {
        maxRetriesPerRequest: 3,
        retryStrategy: (times) => Math.min(times * 500, 5000),
        lazyConnect: true,
      });

      await redis.connect();
      console.log("[state] ✔ Connected to Redis");
      return redis;
    } catch (err) {
      console.error(`[state] Attempt failed: ${err.message}`);
      if (redis) {
        try { redis.disconnect(); } catch (_) { /* ignore */ }
      }
      if (attempt === retries) throw err;
      await new Promise((r) => setTimeout(r, delayMs * attempt));
    }
  }
}

// ── Idempotency ─────────────────────────────────────────────

/**
 * Atomically mark a message_id as processed using SETNX.
 * Returns true if the key was newly set (first time), false if duplicate.
 */
async function markProcessed(messageId) {
  const result = await redis.setnx(`${PROCESSED_PREFIX}${messageId}`, Date.now());
  return result === 1;
}

/**
 * Check if a message_id has already been processed.
 */
async function isProcessed(messageId) {
  const exists = await redis.exists(`${PROCESSED_PREFIX}${messageId}`);
  return exists === 1;
}

// ── Retry Tracking ──────────────────────────────────────────

/**
 * Atomically increment and return the new retry count for a message_id.
 */
async function incrementRetryCount(messageId) {
  const count = await redis.incr(`${RETRY_PREFIX}${messageId}`);
  // Auto-expire after 1 hour to prevent stale keys accumulating
  await redis.expire(`${RETRY_PREFIX}${messageId}`, 3600);
  return count;
}

/**
 * Get the current retry count for a message_id (0 if never retried).
 */
async function getRetryCount(messageId) {
  const count = await redis.get(`${RETRY_PREFIX}${messageId}`);
  return count ? parseInt(count, 10) : 0;
}

/**
 * Remove the retry counter for a message_id (on success or after DLQ routing).
 */
async function clearRetryCount(messageId) {
  await redis.del(`${RETRY_PREFIX}${messageId}`);
}

function getClient() {
  return redis;
}

module.exports = {
  connectStateStore,
  markProcessed,
  isProcessed,
  incrementRetryCount,
  getRetryCount,
  clearRetryCount,
  getClient,
};
