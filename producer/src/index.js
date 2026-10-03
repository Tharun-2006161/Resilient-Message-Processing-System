/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  Galactic Message Relay — Producer HTTP API                 ║
 * ║  POST /command  →  validates & enqueues mission commands    ║
 * ║  GET  /status   →  returns queue depth for primary & poison ║
 * ╚══════════════════════════════════════════════════════════════╝
 */

const express = require("express");
const axios = require("axios");
const { connectBroker, publishToQueue, PRIMARY_QUEUE, POISON_QUEUE } = require("./broker");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const RABBITMQ_MGMT_URL = process.env.RABBITMQ_MGMT_URL || "http://localhost:15672";
const RABBITMQ_USER = process.env.RABBITMQ_USER || "galaxy";
const RABBITMQ_PASS = process.env.RABBITMQ_PASS || "starfleet";

// ── Health Probe ──────────────────────────────────────────────
app.get("/health", (_req, res) => res.json({ status: "ok" }));

// ── POST /command ─────────────────────────────────────────────
app.post("/command", async (req, res) => {
  const { message_id, payload, simulate_error } = req.body;

  // Strict validation — both message_id and payload are required
  if (
    message_id === undefined ||
    message_id === null ||
    message_id === "" ||
    payload === undefined ||
    payload === null ||
    payload === ""
  ) {
    return res.status(400).json({
      error: "Bad Request",
      detail: "Both 'message_id' (string) and 'payload' (string) are required.",
    });
  }

  try {
    const message = {
      message_id: String(message_id),
      payload: String(payload),
      simulate_error: Boolean(simulate_error),
      enqueued_at: new Date().toISOString(),
    };

    // Publish to primary queue — message is persisted by RabbitMQ
    publishToQueue(Buffer.from(JSON.stringify(message)));

    return res.status(202).json({ status: "queued", message_id: String(message_id) });
  } catch (err) {
    console.error("[producer] Failed to enqueue:", err.message);
    return res.status(500).json({ error: "Failed to enqueue message" });
  }
});

// ── GET /status ───────────────────────────────────────────────
app.get("/status", async (_req, res) => {
  try {
    const auth = { username: RABBITMQ_USER, password: RABBITMQ_PASS };

    const [primaryRes, poisonRes] = await Promise.all([
      axios.get(
        `${RABBITMQ_MGMT_URL}/api/queues/%2F/${encodeURIComponent(PRIMARY_QUEUE)}`,
        { auth }
      ),
      axios.get(
        `${RABBITMQ_MGMT_URL}/api/queues/%2F/${encodeURIComponent(POISON_QUEUE)}`,
        { auth }
      ),
    ]);

    return res.json({
      primary_queue_count: primaryRes.data.messages ?? 0,
      poison_queue_count: poisonRes.data.messages ?? 0,
    });
  } catch (err) {
    console.error("[producer] /status error:", err.message);
    return res.status(500).json({ error: "Unable to fetch queue depths" });
  }
});

// ── Boot Sequence ─────────────────────────────────────────────
(async () => {
  try {
    await connectBroker();
    app.listen(PORT, () => {
      console.log(`\n🚀  Galactic Producer API listening on port ${PORT}`);
      console.log(`    POST /command  — enqueue a mission command`);
      console.log(`    GET  /status   — inspect queue depths\n`);
    });
  } catch (err) {
    console.error("[producer] Fatal — could not start:", err.message);
    process.exit(1);
  }
})();
