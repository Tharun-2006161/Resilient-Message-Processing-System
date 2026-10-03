/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  Galactic Message Relay — Broker Connection (Consumer)      ║
 * ║  Reusable RabbitMQ connector with exponential back-off      ║
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
 * Connect to RabbitMQ and mirror the producer's topology declarations
 * so that the consumer can self-heal even if it boots first.
 */
async function connectBroker(retries = 15, delayMs = 3000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      console.log(`[broker] Connecting to RabbitMQ (attempt ${attempt}/${retries})…`);
      connection = await amqplib.connect(RABBITMQ_URL);
      channel = await connection.createChannel();

      // ── Declare topology (idempotent — safe to repeat) ──
      await channel.assertExchange(DLX_EXCHANGE, "direct", { durable: true });
      await channel.assertQueue(POISON_QUEUE, { durable: true });
      await channel.bindQueue(POISON_QUEUE, DLX_EXCHANGE, "dead");

      await channel.assertQueue(PRIMARY_QUEUE, {
        durable: true,
        arguments: {
          "x-dead-letter-exchange": DLX_EXCHANGE,
          "x-dead-letter-routing-key": "dead",
        },
      });

      // Prefetch 1 — process one message at a time
      await channel.prefetch(1);

      connection.on("error", (err) => {
        console.error("[broker] Connection error:", err.message);
      });
      connection.on("close", () => {
        console.warn("[broker] Connection closed");
        connection = null;
        channel = null;
      });

      console.log("[broker] ✔ Connected, topology declared, prefetch=1");
      return { connection, channel };
    } catch (err) {
      console.error(`[broker] Attempt failed: ${err.message}`);
      if (attempt === retries) throw err;
      await sleep(delayMs * attempt);
    }
  }
}

function getChannel() {
  return channel;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

module.exports = { connectBroker, getChannel, PRIMARY_QUEUE, POISON_QUEUE, DLX_EXCHANGE };
