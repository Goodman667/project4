from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse


router = APIRouter(include_in_schema=False)


DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Broker Control Panel</title>
  <style>
    :root {
      --bg: #f5f1e8;
      --panel: rgba(255, 252, 246, 0.88);
      --panel-strong: #ffffff;
      --text: #1d2b33;
      --muted: #60717a;
      --line: rgba(29, 43, 51, 0.12);
      --accent: #0d9488;
      --accent-strong: #0f766e;
      --warn: #b45309;
      --danger: #b91c1c;
      --shadow: 0 20px 45px rgba(29, 43, 51, 0.12);
      --radius: 22px;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: "Trebuchet MS", "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(13, 148, 136, 0.16), transparent 30%),
        radial-gradient(circle at top right, rgba(14, 116, 144, 0.12), transparent 28%),
        linear-gradient(180deg, #fcfaf5 0%, var(--bg) 100%);
    }

    .shell {
      width: min(1180px, calc(100vw - 32px));
      margin: 24px auto 40px;
    }

    .hero {
      display: grid;
      grid-template-columns: 1.4fr 1fr;
      gap: 18px;
      margin-bottom: 18px;
    }

    .panel {
      background: var(--panel);
      backdrop-filter: blur(10px);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
    }

    .hero-card {
      padding: 28px;
      min-height: 210px;
    }

    h1, h2, h3 {
      margin: 0;
      font-family: "Aptos Display", "Trebuchet MS", sans-serif;
      letter-spacing: -0.03em;
    }

    h1 {
      font-size: clamp(2rem, 4vw, 3.4rem);
      line-height: 0.94;
      max-width: 12ch;
      margin-bottom: 18px;
    }

    h2 {
      font-size: 1.15rem;
      margin-bottom: 14px;
    }

    p {
      margin: 0;
      color: var(--muted);
      line-height: 1.55;
    }

    .hero-copy {
      max-width: 56ch;
      margin-bottom: 18px;
    }

    .badge-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid rgba(13, 148, 136, 0.18);
      background: rgba(13, 148, 136, 0.08);
      color: var(--accent-strong);
      font-size: 0.92rem;
      font-weight: 600;
    }

    .quick-links {
      display: grid;
      gap: 12px;
      align-content: start;
    }

    .link-card {
      display: block;
      padding: 18px;
      text-decoration: none;
      color: inherit;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.74);
      transition: transform 0.18s ease, border-color 0.18s ease;
    }

    .link-card:hover {
      transform: translateY(-2px);
      border-color: rgba(13, 148, 136, 0.3);
    }

    .link-card strong {
      display: block;
      margin-bottom: 6px;
      font-size: 1rem;
    }

    .grid {
      display: grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap: 18px;
    }

    .stack {
      display: grid;
      gap: 18px;
    }

    .section {
      padding: 22px;
    }

    .fields {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 14px;
    }

    .field {
      display: grid;
      gap: 8px;
    }

    label {
      font-size: 0.9rem;
      font-weight: 700;
      color: #39515e;
    }

    input, textarea {
      width: 100%;
      border: 1px solid var(--line);
      background: var(--panel-strong);
      border-radius: 14px;
      padding: 12px 14px;
      font: inherit;
      color: var(--text);
      outline: none;
    }

    textarea {
      min-height: 140px;
      resize: vertical;
    }

    input:focus, textarea:focus {
      border-color: rgba(13, 148, 136, 0.55);
      box-shadow: 0 0 0 4px rgba(13, 148, 136, 0.12);
    }

    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 16px;
    }

    button {
      border: 0;
      border-radius: 14px;
      padding: 12px 16px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.15s ease, opacity 0.15s ease;
    }

    button:hover {
      transform: translateY(-1px);
    }

    button:disabled {
      opacity: 0.5;
      cursor: not-allowed;
      transform: none;
    }

    .btn-primary {
      background: linear-gradient(135deg, var(--accent), var(--accent-strong));
      color: white;
    }

    .btn-secondary {
      background: rgba(14, 116, 144, 0.12);
      color: #155e75;
    }

    .btn-neutral {
      background: rgba(29, 43, 51, 0.08);
      color: var(--text);
    }

    .btn-danger {
      background: rgba(185, 28, 28, 0.12);
      color: var(--danger);
    }

    .btn-warn {
      background: rgba(180, 83, 9, 0.12);
      color: var(--warn);
    }

    .metric-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 14px;
    }

    .metric {
      padding: 14px;
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.76);
      border: 1px solid var(--line);
    }

    .metric small {
      display: block;
      color: var(--muted);
      margin-bottom: 8px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.72rem;
    }

    .metric strong {
      font-size: 1.35rem;
    }

    pre {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      background: rgba(29, 43, 51, 0.95);
      color: #dff7f4;
      padding: 16px;
      border-radius: 16px;
      font-family: Consolas, "Cascadia Code", monospace;
      font-size: 0.92rem;
      line-height: 1.5;
      min-height: 150px;
    }

    .log {
      min-height: 220px;
      max-height: 360px;
      overflow: auto;
    }

    .hint {
      margin-top: 10px;
      font-size: 0.9rem;
      color: var(--muted);
    }

    .footer-note {
      margin-top: 18px;
      padding: 16px 18px;
      border-radius: 18px;
      background: rgba(13, 148, 136, 0.08);
      border: 1px solid rgba(13, 148, 136, 0.14);
      color: #115e59;
      font-weight: 600;
    }

    @media (max-width: 980px) {
      .hero,
      .grid,
      .fields {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div class="panel hero-card">
        <h1>Broker Control Panel</h1>
        <p class="hero-copy">
          A browser-based demo console for the lightweight message broker.
          Create topics, publish messages, consume one message at a time,
          ack or nack, and inspect queue depth plus per-topic metrics from one page.
        </p>
        <div class="badge-row">
          <span class="badge">Single Machine</span>
          <span class="badge">In-Memory Queue</span>
          <span class="badge">Topic-Based</span>
          <span class="badge">Course Demo Ready</span>
        </div>
      </div>
      <div class="panel hero-card quick-links">
        <a class="link-card" href="/docs" target="_blank" rel="noreferrer">
          <strong>Swagger Docs</strong>
          <span>Open the raw API documentation when you need endpoint details.</span>
        </a>
        <a class="link-card" href="/health" target="_blank" rel="noreferrer">
          <strong>Health Check</strong>
          <span>Quickly verify the service is running before the demo starts.</span>
        </a>
        <a class="link-card" href="/api-info" target="_blank" rel="noreferrer">
          <strong>API Info</strong>
          <span>See the backend service summary and available utility routes.</span>
        </a>
      </div>
    </section>

    <section class="grid">
      <div class="stack">
        <div class="panel section">
          <h2>Topic Setup</h2>
          <div class="fields">
            <div class="field">
              <label for="topic">Topic</label>
              <input id="topic" value="demo-topic" />
            </div>
            <div class="field">
              <label for="consumerId">Consumer ID</label>
              <input id="consumerId" value="consumer-1" />
            </div>
          </div>
          <div class="actions">
            <button class="btn-primary" id="createTopicBtn">Create Topic</button>
            <button class="btn-secondary" id="listTopicsBtn">Refresh Topics</button>
            <button class="btn-neutral" id="depthBtn">Check Depth</button>
            <button class="btn-neutral" id="metricsBtn">Refresh Metrics</button>
          </div>
          <p class="hint">Tip: create the topic once, then use the same topic name for publish and consume.</p>
        </div>

        <div class="panel section">
          <h2>Publish</h2>
          <div class="field">
            <label for="payload">Payload</label>
            <textarea id="payload">{
  "text": "hello broker",
  "source": "dashboard"
}</textarea>
          </div>
          <div class="actions">
            <button class="btn-primary" id="publishBtn">Publish Message</button>
          </div>
          <p class="hint">The payload can be plain text or JSON. If valid JSON is entered, it will be sent as JSON data.</p>
        </div>

        <div class="panel section">
          <h2>Consume And Confirm</h2>
          <div class="actions">
            <button class="btn-primary" id="consumeBtn">Consume One</button>
            <button class="btn-secondary" id="ackBtn" disabled>Ack Last</button>
            <button class="btn-warn" id="nackBtn" disabled>Nack Last</button>
          </div>
          <div class="hint">Last consumed message:</div>
          <pre id="lastMessage">No message consumed yet.</pre>
        </div>
      </div>

      <div class="stack">
        <div class="panel section">
          <h2>Metrics Snapshot</h2>
          <div class="metric-grid">
            <div class="metric"><small>Queue Depth</small><strong id="metricDepth">0</strong></div>
            <div class="metric"><small>Total Published</small><strong id="metricPublished">0</strong></div>
            <div class="metric"><small>Total Consumed</small><strong id="metricConsumed">0</strong></div>
            <div class="metric"><small>Total Acked</small><strong id="metricAcked">0</strong></div>
            <div class="metric"><small>Total Nacked</small><strong id="metricNacked">0</strong></div>
            <div class="metric"><small>Total Retries</small><strong id="metricRetries">0</strong></div>
            <div class="metric"><small>Message Rate</small><strong id="metricRate">0</strong></div>
            <div class="metric"><small>Byte Throughput</small><strong id="metricBytes">0</strong></div>
          </div>
          <div class="footer-note" id="backpressureState">
            Backpressure status will appear here after the first metrics refresh.
          </div>
        </div>

        <div class="panel section">
          <h2>Topics</h2>
          <pre id="topicsView">[]</pre>
        </div>

        <div class="panel section">
          <h2>Activity Log</h2>
          <pre id="activityLog" class="log">Dashboard ready.</pre>
        </div>
      </div>
    </section>
  </main>

  <script>
    const topicInput = document.getElementById("topic");
    const consumerInput = document.getElementById("consumerId");
    const payloadInput = document.getElementById("payload");
    const lastMessageView = document.getElementById("lastMessage");
    const topicsView = document.getElementById("topicsView");
    const activityLog = document.getElementById("activityLog");
    const backpressureState = document.getElementById("backpressureState");

    const ackBtn = document.getElementById("ackBtn");
    const nackBtn = document.getElementById("nackBtn");

    const state = {
      lastMessageId: null,
      lastMessage: null,
    };

    function currentTopic() {
      return topicInput.value.trim();
    }

    function currentConsumerId() {
      return consumerInput.value.trim() || "consumer-1";
    }

    function log(message) {
      const timestamp = new Date().toLocaleTimeString();
      activityLog.textContent = `[${timestamp}] ${message}\\n` + activityLog.textContent;
    }

    function setMetric(id, value) {
      document.getElementById(id).textContent = String(value);
    }

    function updateMessageButtons() {
      const enabled = Boolean(state.lastMessageId);
      ackBtn.disabled = !enabled;
      nackBtn.disabled = !enabled;
    }

    function parsePayload() {
      const raw = payloadInput.value.trim();
      if (!raw) {
        return "";
      }

      try {
        return JSON.parse(raw);
      } catch {
        return raw;
      }
    }

    async function requestJson(url, options = {}) {
      const response = await fetch(url, {
        headers: {
          "Content-Type": "application/json",
          ...(options.headers || {}),
        },
        ...options,
      });

      const text = await response.text();
      let data;
      try {
        data = text ? JSON.parse(text) : {};
      } catch {
        data = { raw: text };
      }

      if (!response.ok) {
        const detail = data && data.detail ? data.detail : response.statusText;
        throw new Error(`HTTP ${response.status}: ${detail}`);
      }

      return data;
    }

    async function createTopic() {
      const topic = currentTopic();
      if (!topic) {
        log("Please enter a topic name first.");
        return;
      }

      const data = await requestJson(`/topics/${encodeURIComponent(topic)}`, {
        method: "POST",
      });
      log(`Topic ready: ${data.topic} (${data.detail})`);
      await refreshTopics();
      await refreshMetrics();
    }

    async function refreshTopics() {
      const data = await requestJson("/topics");
      topicsView.textContent = JSON.stringify(data.topics, null, 2);
      log(`Loaded topic list (${data.topics.length} total).`);
    }

    async function publishMessage() {
      const topic = currentTopic();
      const payload = parsePayload();
      const data = await requestJson(`/topics/${encodeURIComponent(topic)}/publish`, {
        method: "POST",
        body: JSON.stringify({ payload }),
      });
      log(`Published message ${data.message.id} to topic '${topic}'.`);
      await refreshMetrics();
    }

    async function consumeMessage() {
      const topic = currentTopic();
      const consumerId = currentConsumerId();
      const data = await requestJson(
        `/topics/${encodeURIComponent(topic)}/consume?consumer_id=${encodeURIComponent(consumerId)}`
      );

      if (!data.message) {
        state.lastMessageId = null;
        state.lastMessage = null;
        lastMessageView.textContent = "No messages available right now.";
        updateMessageButtons();
        log(`No messages available for topic '${topic}'.`);
        await refreshMetrics();
        return;
      }

      state.lastMessageId = data.message.id;
      state.lastMessage = data.message;
      lastMessageView.textContent = JSON.stringify(data.message, null, 2);
      updateMessageButtons();
      log(`Consumed message ${data.message.id} from topic '${topic}'.`);
      await refreshMetrics();
    }

    async function ackLast() {
      if (!state.lastMessageId) {
        log("No consumed message available to ack.");
        return;
      }

      const messageId = state.lastMessageId;
      const data = await requestJson(`/messages/${encodeURIComponent(messageId)}/ack`, {
        method: "POST",
      });
      log(data.detail);
      state.lastMessageId = null;
      state.lastMessage = null;
      lastMessageView.textContent = "Last message acknowledged.";
      updateMessageButtons();
      await refreshMetrics();
    }

    async function nackLast() {
      if (!state.lastMessageId) {
        log("No consumed message available to nack.");
        return;
      }

      const messageId = state.lastMessageId;
      const data = await requestJson(`/messages/${encodeURIComponent(messageId)}/nack`, {
        method: "POST",
      });
      log(data.detail);
      state.lastMessageId = null;
      state.lastMessage = null;
      lastMessageView.textContent = "Last message requeued by nack.";
      updateMessageButtons();
      await refreshMetrics();
    }

    async function refreshDepth() {
      const topic = currentTopic();
      const data = await requestJson(`/topics/${encodeURIComponent(topic)}/depth`);
      setMetric("metricDepth", data.depth);
      log(`Queue depth for '${topic}' is ${data.depth}.`);
    }

    async function refreshMetrics() {
      const topic = currentTopic();
      const data = await requestJson(`/topics/${encodeURIComponent(topic)}/metrics`);
      const metrics = data.metrics;
      const backpressure = data.backpressure;

      setMetric("metricDepth", metrics.current_queue_depth);
      setMetric("metricPublished", metrics.total_published);
      setMetric("metricConsumed", metrics.total_consumed);
      setMetric("metricAcked", metrics.total_acked);
      setMetric("metricNacked", metrics.total_nacked);
      setMetric("metricRetries", metrics.total_retries);
      setMetric("metricRate", metrics.message_rate);
      setMetric("metricBytes", metrics.byte_throughput);

      backpressureState.textContent =
        `Backpressure: ${backpressure.throttled ? "THROTTLED" : "ACCEPTING"} | ` +
        `queue_depth=${backpressure.queue_depth} | ` +
        `max_queue_depth=${backpressure.max_queue_depth}`;

      log(`Metrics refreshed for topic '${topic}'.`);
    }

    async function safeRun(action) {
      try {
        await action();
      } catch (error) {
        log(error.message);
      }
    }

    document.getElementById("createTopicBtn").addEventListener("click", () => safeRun(createTopic));
    document.getElementById("listTopicsBtn").addEventListener("click", () => safeRun(refreshTopics));
    document.getElementById("publishBtn").addEventListener("click", () => safeRun(publishMessage));
    document.getElementById("consumeBtn").addEventListener("click", () => safeRun(consumeMessage));
    document.getElementById("ackBtn").addEventListener("click", () => safeRun(ackLast));
    document.getElementById("nackBtn").addEventListener("click", () => safeRun(nackLast));
    document.getElementById("depthBtn").addEventListener("click", () => safeRun(refreshDepth));
    document.getElementById("metricsBtn").addEventListener("click", () => safeRun(refreshMetrics));

    updateMessageButtons();
    refreshTopics().catch(() => {});
  </script>
</body>
</html>
"""


@router.get("/", response_class=RedirectResponse)
def root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/dashboard", status_code=302)


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    return HTMLResponse(DASHBOARD_HTML)
