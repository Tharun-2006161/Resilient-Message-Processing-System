/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  Galactic Message Relay — Broker Connection Manager         ║
 * ║  Provides a resilient, reusable RabbitMQ connection/channel ║
 * ╚══════════════════════════════════════════════════════════════╝
 */

const amqplib = require("amqplib");

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://galaxy:starfleet@localhost:5672";
const PRIMARY_QUEUE = process.env.PRIMARY_QUEUE || "primary_queue";
const POISON_QUEUE = process.env.POISON_QUEUE || "poison_queue";
const DLX_EXCHANGE = "dlx_exchange";

let connection = null;
let channel = null;

/**
 * Connect to RabbitMQ and declare all required topology:
 *  • Dead-Letter Exchange (direct)
 *  • Poison Queue bound to the DLX
 *  • Primary Queue with DLX arguments
 *
 * Retries on failure with exponential back-off.
 */
async function connectBroker(retries = 10, delayMs = 3000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      console.log(`[broker] Connecting to RabbitMQ (attempt ${attempt}/${retries})…`);
      connection = await amqplib.connect(RABBITMQ_URL);
      channel = await connection.createChannel();

      // ── Dead-Letter Exchange ────────────────────────────
      await channel.assertExchange(DLX_EXCHANGE, "direct", { durable: true });

      // ── Poison Queue ────────────────────────────────────
      await channel.assertQueue(POISON_QUEUE, { durable: true });
      await channel.bindQueue(POISON_QUEUE, DLX_EXCHANGE, "dead");

      // ── Primary Queue (routes rejected msgs → DLX) ─────
      await channel.assertQueue(PRIMARY_QUEUE, {
        durable: true,
        arguments: {
          "x-dead-letter-exchange": DLX_EXCHANGE,
          "x-dead-letter-routing-key": "dead",
        },
      });

      // Graceful shutdown handlers
      connection.on("error", (err) => {
        console.error("[broker] Connection error:", err.message);
      });
      connection.on("close", () => {
        console.warn("[broker] Connection closed — will reconnect on next publish");
        connection = null;
        channel = null;
      });

      console.log("[broker] ✔ Connected & topology declared");
      return { connection, channel };
    } catch (err) {
      console.error(`[broker] Connection failed: ${err.message}`);
      if (attempt === retries) throw err;
      await sleep(delayMs * attempt); // exponential-ish backoff
    }
  }
}

/** Publish a Buffer to the primary queue (persistent). */
async function publishToQueue(messageBuffer) {
  if (!channel) throw new Error("Broker channel not available");
  return channel.sendToQueue(PRIMARY_QUEUE, messageBuffer, { persistent: true });
}

/** Return the current channel (for management queries, etc.). */
function getChannel() {
  return channel;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

module.exports = { connectBroker, publishToQueue, getChannel, PRIMARY_QUEUE, POISON_QUEUE };
