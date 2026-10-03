/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  Galactic Message Relay — Consumer Worker                   ║
 * ║                                                             ║
 * ║  Continuously consumes from primary_queue with:             ║
 * ║    • Idempotency via Redis SETNX                            ║
 * ║    • Manual ACK / NACK                                      ║
 * ║    • Retry count tracked in Redis                           ║
 * ║    • Dead-letter routing after MAX_RETRIES failures         ║
 * ╚══════════════════════════════════════════════════════════════╝
 */

const { connectBroker, getChannel, PRIMARY_QUEUE, POISON_QUEUE } = require("./broker");
const {
  connectStateStore,
  markProcessed,
  isProcessed,
  incrementRetryCount,
  getRetryCount,
  clearRetryCount,
  getClient,
} = require("./stateStore");

const MAX_RETRIES = parseInt(process.env.MAX_RETRIES, 10) || 3;

// Small delay between retries to avoid tight retry loops
const RETRY_DELAY_MS = 500;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Main message handler — invoked for every delivery from primary_queue.
 */
async function handleMessage(msg) {
  const channel = getChannel();
  if (!msg) return; // consumer cancelled

  let data;
  try {
    data = JSON.parse(msg.content.toString());
  } catch (parseErr) {
    // Unparseable payload — immediately dead-letter, no point retrying
    console.error("[consumer] ✘ Malformed JSON — routing to poison queue");
    channel.sendToQueue(POISON_QUEUE, msg.content, { persistent: true });
    channel.ack(msg);
    return;
  }

  const { message_id, payload, simulate_error } = data;
  const currentRetries = await getRetryCount(message_id);

  console.log(
    `\n[consumer] ▶ Received message_id=${message_id}  attempt=${currentRetries + 1}/${MAX_RETRIES + 1}`
  );

  // ── 1. Idempotency Check ──────────────────────────────────
  try {
    const alreadyProcessed = await isProcessed(message_id);
    if (alreadyProcessed) {
      console.warn(`[consumer] ⚠ Duplicate ignored (message_id=${message_id})`);
      channel.ack(msg);
      return;
    }
  } catch (stateErr) {
    // State store unavailable — NACK and requeue for later retry
    console.error("[consumer] ✘ State store check failed:", stateErr.message);
    channel.nack(msg, false, true);
    return;
  }

  // ── 2. Process Payload ────────────────────────────────────
  try {
    // Simulated error path — throws before persisting to state store
    if (simulate_error) {
      throw new Error(`Simulated transient failure for message_id=${message_id}`);
    }

    // ── Business Logic (mission processing) ──────────────
    console.log(`[consumer] 🛰  Processing mission command: "${payload}"`);

    // ── 3. Persist to State Store (atomic SETNX) ─────────
    const wasNew = await markProcessed(message_id);
    if (!wasNew) {
      // Race-condition safety: another worker beat us to it
      console.warn(`[consumer] ⚠ Race-condition duplicate caught (message_id=${message_id})`);
      channel.ack(msg);
      return;
    }

    // Clean up retry counter on success
    await clearRetryCount(message_id);

    // ── 4. Manual ACK — only after everything succeeded ──
    channel.ack(msg);
    console.log(`[consumer] ✔ Successfully processed message_id=${message_id}`);
  } catch (processingErr) {
    console.error(
      `[consumer] ✘ Processing failed (attempt ${currentRetries + 1}): ${processingErr.message}`
    );

    // Increment the retry counter in Redis
    const newRetryCount = await incrementRetryCount(message_id);

    // ── Retry or Dead-Letter Decision ────────────────────
    if (newRetryCount >= MAX_RETRIES) {
      // Exhausted all retries → route to poison queue
      console.error(
        `[consumer] 💀 Max retries (${MAX_RETRIES}) exceeded — routing to poison queue`
      );

      // Manually publish to poison queue and ACK from primary
      channel.sendToQueue(POISON_QUEUE, msg.content, { persistent: true });
      channel.ack(msg);

      // Clean up retry counter
      await clearRetryCount(message_id);
    } else {
      // Still have retries left — NACK with requeue
      console.warn(
        `[consumer] ↻ Retry ${newRetryCount}/${MAX_RETRIES} — requeuing after ${RETRY_DELAY_MS}ms delay`
      );
      await sleep(RETRY_DELAY_MS);
      channel.nack(msg, false, true); // nack, requeue
    }
  }
}

// ── Boot ─────────────────────────────────────────────────────
(async () => {
  try {
    await connectStateStore();
    await connectBroker();

    const channel = getChannel();

    // Begin consuming with manual ack (noAck: false)
    await channel.consume(PRIMARY_QUEUE, handleMessage, { noAck: false });

    console.log(`\n🛰  Galactic Consumer Worker online — listening on [${PRIMARY_QUEUE}]`);
    console.log(`   Max retries per message: ${MAX_RETRIES}`);
    console.log(`   Poison queue: ${POISON_QUEUE}\n`);
  } catch (err) {
    console.error("[consumer] Fatal — could not start:", err.message);
    process.exit(1);
  }
})();
