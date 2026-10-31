"""Kafka consumer + live dashboard.  python dashboard.py   (or --demo to run with no Kafka)"""
import argparse, json, threading, time
from collections import Counter, deque
from flask import Flask, Response, render_template
from common import EventGenerator, encode, BOOTSTRAP, TOPIC


class Aggregator:
    def __init__(self):
        self.lock = threading.Lock()
        self.total = self.bytes = self.pulls = 0
        self.ranks, self.banners, self.types = Counter(), Counter(), Counter()
        self.items, self.packs, self.pity_hist = Counter(), Counter(), Counter()
        self.spend, self.pullers = Counter(), Counter()
        self.revenue = 0
        self.recent_s = deque(maxlen=8)
        self.eps_hist = deque([0] * 120, maxlen=120)
        self.lag, self.started, self._last_total = 0, time.time(), 0
        self.latest = "{}"

    def update_many(self, evs):                        # evs: list of (dict, nbytes)
        with self.lock:
            for e, nb in evs:
                self.total += 1; self.bytes += nb
                if e["event_type"] == "pull":
                    self.pulls += 1
                    self.ranks[e["rank"]] += 1; self.banners[e["banner"]] += 1
                    self.types[e["item_type"]] += 1; self.pullers[e["username"]] += 1
                    if e["rank"] == "S":
                        self.items[e["item_name"]] += 1
                        self.pity_hist[min(e["pity_count"] // 10 * 10, 90)] += 1
                        self.recent_s.appendleft({"user": e["username"], "item": e["item_name"],
                                                  "banner": e["banner"], "pity": e["pity_count"],
                                                  "featured": e["is_featured"]})
                else:
                    self.spend[e["username"]] += e["price_php"]; self.revenue += e["price_php"]
                    self.packs[e["pack"]] += 1

    def tick(self):                                    # called once per second
        with self.lock:
            self.eps_hist.append(self.total - self._last_total); self._last_total = self.total
            n = max(self.pulls, 1)
            self.latest = json.dumps({
                "eps": self.eps_hist[-1], "eps_hist": list(self.eps_hist), "total": self.total,
                "mb": round(self.bytes / 1e6, 2), "lag": self.lag, "pulls": self.pulls,
                "uptime": int(time.time() - self.started), "revenue": self.revenue,
                "s_rate": round(100 * self.ranks["S"] / n, 2),
                "ranks": dict(self.ranks), "banners": dict(self.banners), "types": dict(self.types),
                "top_s_items": self.items.most_common(8), "packs": self.packs.most_common(),
                "top_spenders": self.spend.most_common(10), "top_pullers": self.pullers.most_common(5),
                "pity": [[k, self.pity_hist[k]] for k in range(0, 100, 10)], "recent_s": list(self.recent_s)})


agg = Aggregator()
app = Flask(__name__)

@app.route("/")
def index(): return render_template("dashboard.html")

@app.route("/stream")
def stream():
    def gen():
        while True:
            yield f"data: {agg.latest}\n\n"; time.sleep(1)
    return Response(gen(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache"})

def ticker():
    while True:
        time.sleep(1); agg.tick()

def kafka_loop(group, from_start):
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "enable.auto.commit": True,
                  "auto.offset.reset": "earliest" if from_start else "latest", "fetch.min.bytes": 65536})
    c.subscribe([TOPIC]); last_lag = 0
    while True:
        msgs = c.consume(num_messages=2000, timeout=0.3)
        batch = []
        for m in msgs:
            if m.error(): continue
            v = m.value(); batch.append((json.loads(v), len(v)))
        if batch: agg.update_many(batch)
        if time.time() - last_lag > 2:                 # consumer lag = latest offset - our position
            last_lag = time.time(); lag = 0
            try:
                for tp in c.position(c.assignment()):
                    lo, hi = c.get_watermark_offsets(tp, timeout=1)
                    if tp.offset >= 0: lag += max(hi - tp.offset, 0)
                agg.lag = lag
            except Exception: pass

def demo_loop(rate):
    g, carry, t0 = EventGenerator(), 0.0, time.time()
    while True:
        now = time.time(); carry = min(carry + rate * (now - t0), rate); t0 = now
        out = []
        while carry >= 1:
            b = g.next_batch(); carry -= len(b); out += [(e, len(encode(e))) for e in b]
        agg.update_many(out); time.sleep(0.05)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true", help="fake in-process stream, no Kafka needed")
    ap.add_argument("--demo-rate", type=int, default=3000)
    ap.add_argument("--group", default="zzz-dashboard")
    ap.add_argument("--from-start", action="store_true", help="replay retained Kafka data")
    ap.add_argument("--port", type=int, default=5000)
    a = ap.parse_args()
    threading.Thread(target=ticker, daemon=True).start()
    threading.Thread(target=demo_loop if a.demo else kafka_loop,
                     args=(a.demo_rate,) if a.demo else (a.group, a.from_start), daemon=True).start()
    print(f"Dashboard: http://127.0.0.1:{a.port}")
    app.run(host="127.0.0.1", port=a.port, threaded=True)
