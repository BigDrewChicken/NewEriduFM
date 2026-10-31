"""Shared config + synthetic event generator for the ZZZ pull stream."""
import csv, json, os, random, time, uuid
from collections import defaultdict
try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
TOPIC = "zzz.events"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
PULL_COST = 160                                   # polychrome per pull
BANNERS = ["Exclusive Channel", "W-Engine Channel", "Stable Channel"]
BANNER_W = [0.5, 0.3, 0.2]
# (pack name, polychrome, price PHP) - illustrative prices, not official
PACKS = [("Starter Pack", 60, 59), ("Small Pack", 300, 299), ("Medium Pack", 980, 949),
         ("Large Pack", 1980, 1899), ("XL Pack", 3280, 2999), ("Whale Pack", 6480, 5999)]
PACK_W = [0.35, 0.30, 0.18, 0.10, 0.05, 0.02]
PAYMENTS = ["GCash", "Maya", "Card", "GrabPay"]
PAY_W = [0.55, 0.2, 0.15, 0.1]


class EventGenerator:
    """Produces batches: a 1-pull, a 10-pull (as 1/10 pull events) or a top-up event."""

    def __init__(self, s_rate=0.05, topup_prob=0.04, seed=None):
        self.r = random.Random(seed)
        self.s_rate, self.topup_prob = s_rate, topup_prob
        items = list(csv.DictReader(open(os.path.join(HERE, "data", "zzz_possible_items.csv"), encoding="utf-8")))
        users = json.load(open(os.path.join(HERE, "data", "username_id_dictionary.json"), encoding="utf-8"))
        self.users = list(users.items())
        n = len(self.users)
        w = [self.r.paretovariate(1.5) for _ in range(n)]       # heavy-tailed activity -> a few whales
        self.pull_pool = self.r.choices(range(n), weights=w, k=200_000)
        self.spend_pool = self.r.choices(range(n), weights=[x ** 1.4 for x in w], k=200_000)
        self.pi = self.si = 0
        self.pools = {b: {k: [i for i in items if b in i["banners"].split("|") and i["rank"] == k]
                          for k in ("S", "A")} for b in BANNERS}
        self.featured = [i for i in self.pools["Exclusive Channel"]["S"] if i["item_name"] == "Phoenix"]
        self.pity = defaultdict(int)

    def _user(self, pool, attr):
        i = getattr(self, attr)
        if i >= len(pool): i = 0
        setattr(self, attr, i + 1)
        return self.users[pool[i]]

    def next_batch(self):
        now = int(time.time() * 1000)
        if self.r.random() < self.topup_prob:
            return [self._topup(now)]
        return self._pulls(now)

    def _topup(self, now):
        name, uid = self._user(self.spend_pool, "si")
        pack = self.r.choices(PACKS, PACK_W)[0]
        return {"event_type": "topup", "event_id": uuid.uuid4().hex, "ts": now, "username": name, "id": uid,
                "pack": pack[0], "polychrome": pack[1], "price_php": pack[2],
                "payment": self.r.choices(PAYMENTS, PAY_W)[0]}

    def _pulls(self, now):
        r = self.r
        name, uid = self._user(self.pull_pool, "pi")
        banner = r.choices(BANNERS, BANNER_W)[0]
        n = 10 if r.random() < 0.35 else 1
        batch_id, evs, ranks = uuid.uuid4().hex, [], []
        key = (uid, banner)
        for k in range(1, n + 1):
            p = self.pity[key] + 1
            rank = "S" if (p >= 90 or r.random() < self.s_rate) else "A"
            self.pity[key] = 0 if rank == "S" else p
            pool, feat = self.pools[banner][rank], False
            if rank == "S" and banner == "Exclusive Channel" and self.featured and r.random() < 0.5:
                pool, feat = self.featured, True
            it = r.choice(pool)
            ranks.append(rank)
            evs.append({"event_type": "pull", "ts": now, "username": name, "id": uid, "banner": banner,
                        "pull_number": k, "pull_id": uuid.uuid4().hex, "batch_id": batch_id, "batch_size": n,
                        "item_id": it["item_id"], "item_name": it["item_name"], "item_type": it["item_type"],
                        "rank": rank, "pity_count": p, "is_featured": feat, "polychrome_spent": PULL_COST})
        for e in evs: e["batch_ranks"] = ranks               # e.g. ["S","A","A",...]
        return evs


def encode(ev):
    return json.dumps(ev, separators=(",", ":")).encode()
