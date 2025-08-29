import feedparser, yaml, csv, os
from datetime import datetime, timezone

with open("config/rss.yml") as f:
    FEEDS = yaml.safe_load(f)["feeds"]

os.makedirs("data", exist_ok=True)
out_path = "data/news_daily.csv"
fields = ["date","source","title","link"]

if not os.path.exists(out_path):
    import csv; csv.DictWriter(open(out_path, "w", newline=""), fields).writeheader()

def one_line(s, n=140): 
    s = " ".join(s.split())
    return s if len(s) <= n else s[:n-1] + "…"

rows = []
for url in FEEDS:
    feed = feedparser.parse(url)
    src = feed.feed.get("title", url)
    for e in feed.entries[:3]:  # keep it small (top 3 per feed)
        rows.append({
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "source": src,
            "title": one_line(e.get("title", "")),
            "link": e.get("link", "")
        })

with open(out_path, "a", newline="") as f:
    w = csv.DictWriter(f, fields); [w.writerow(r) for r in rows]

print(f"Wrote {len(rows)} headlines.")
