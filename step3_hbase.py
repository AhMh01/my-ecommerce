import json, os
from datetime import datetime
from collections import defaultdict

DATA_DIR = "ecommerce_data"

print("=" * 55)
print("  STEP 3: HBase Setup & Data Loading")
print("=" * 55)

print("""
BEFORE running this script, do these steps:

  1. Install Docker Desktop from https://www.docker.com
     (download, install, open it, wait until it says Engine Running)

  2. Open a NEW terminal/Command Prompt and run:

     docker run -d --name hbase-ecommerce ^
       -p 2181:2181 -p 9090:9090 -p 16010:16010 ^
       harisekhon/hbase:latest

  3. Wait 60 seconds, then verify it is running:
     docker ps

  4. Install the Python client:
     pip install happybase

  5. Run this script again: python step3_hbase.py
""")

print("HBase Shell Commands (for your report):")
print("""
  -- open shell with:
  docker exec -it hbase-ecommerce hbase shell

  -- create tables:
  create 'user_sessions', {NAME=>'sess'},{NAME=>'device'},{NAME=>'geo'}
  create 'product_metrics', {NAME=>'views'},{NAME=>'sales'},{NAME=>'cart'}

  -- sample insert:
  put 'user_sessions','user_000042_9999856278','sess:session_id','sess_abc123'
  put 'user_sessions','user_000042_9999856278','sess:conversion_status','converted'
  put 'user_sessions','user_000042_9999856278','device:type','mobile'
  put 'user_sessions','user_000042_9999856278','geo:city','New York'

  -- query all sessions for a user:
  scan 'user_sessions', {STARTROW=>'user_000042_', STOPROW=>'user_000042_~'}

  -- get one row:
  get 'user_sessions', 'user_000042_9999856278'

  -- product metrics for March:
  scan 'product_metrics', {STARTROW=>'prod_00001_2025-03', STOPROW=>'prod_00001_2025-04'}

  -- count rows:
  count 'user_sessions'
""")

try:
    import happybase
    print("Connecting to HBase on localhost:9090 ...")
    conn = happybase.Connection("localhost", port=9090)
    conn.open()
    print("  Connected!")
except Exception as e:
    print(f"  Could not connect: {e}")
    print("  Make sure Docker container is running (see instructions above).")
    print("  Script will exit. Start Docker then re-run.")
    exit()

print("\n[1] Creating tables...")
existing = [t.decode() for t in conn.tables()]
for tbl in ["user_sessions", "product_metrics"]:
    if tbl in existing:
        conn.delete_table(tbl, disable=True)
        print(f"  dropped old {tbl}")
conn.create_table("user_sessions",
    {"sess": {}, "device": {}, "geo": {}})
conn.create_table("product_metrics",
    {"views": {}, "sales": {}, "cart": {}})
print("  done: tables created")

print("\n[2] Loading sessions into HBase...")
all_sessions = []
for fn in sorted(os.listdir(DATA_DIR)):
    if fn.startswith("sessions_") and fn.endswith(".json"):
        with open(os.path.join(DATA_DIR, fn)) as f:
            all_sessions.extend(json.load(f))

tbl_sess  = conn.table("user_sessions")
EPOCH_MAX = 9999999999
batch = tbl_sess.batch(batch_size=500)
count = 0
for sess in all_sessions:
    try:
        ts = int(datetime.fromisoformat(sess["start_time"]).timestamp())
    except:
        ts = 0
    rev_ts  = EPOCH_MAX - ts
    row_key = f"{sess['user_id']}_{rev_ts:010d}".encode()
    batch.put(row_key, {
        b"sess:session_id":        sess["session_id"].encode(),
        b"sess:start_time":        sess["start_time"].encode(),
        b"sess:end_time":          sess.get("end_time","").encode(),
        b"sess:duration_seconds":  str(sess.get("duration_seconds",0)).encode(),
        b"sess:conversion_status": sess.get("conversion_status","").encode(),
        b"sess:referrer":          sess.get("referrer","").encode(),
        b"device:type":    sess["device_profile"].get("type","").encode(),
        b"device:os":      sess["device_profile"].get("os","").encode(),
        b"device:browser": sess["device_profile"].get("browser","").encode(),
        b"geo:city":       sess["geo_data"].get("city","").encode(),
        b"geo:state":      sess["geo_data"].get("state","").encode(),
    })
    count += 1
batch.send()
print(f"  done: {count:,} sessions loaded")

print("\n[3] Computing product metrics...")
with open(os.path.join(DATA_DIR, "transactions.json")) as f:
    transactions = json.load(f)

daily_sales = defaultdict(lambda: {"units":0,"revenue":0.0,"orders":0})
for txn in transactions:
    date = txn["timestamp"][:10]
    for item in txn.get("items",[]):
        k = (item["product_id"], date)
        daily_sales[k]["units"]   += item["quantity"]
        daily_sales[k]["revenue"] += item["subtotal"]
        daily_sales[k]["orders"]  += 1

daily_views = defaultdict(lambda: {"views":0,"duration":0,"cart_adds":0})
for sess in all_sessions:
    date = sess["start_time"][:10]
    for pv in sess.get("page_views",[]):
        if pv.get("product_id"):
            k = (pv["product_id"], date)
            daily_views[k]["views"]    += 1
            daily_views[k]["duration"] += pv.get("view_duration",0)
    for pid in sess.get("cart_contents",{}).keys():
        daily_views[(pid, date)]["cart_adds"] += 1

tbl_prod = conn.table("product_metrics")
all_keys = set(list(daily_sales.keys()) + list(daily_views.keys()))
batch2   = tbl_prod.batch(batch_size=500)
count2   = 0
for (prod_id, date) in all_keys:
    row_key  = f"{prod_id}_{date}".encode()
    row_data = {}
    if (prod_id, date) in daily_views:
        v = daily_views[(prod_id, date)]
        row_data[b"views:view_count"]          = str(v["views"]).encode()
        row_data[b"views:total_view_duration"] = str(v["duration"]).encode()
        row_data[b"cart:add_to_cart_count"]    = str(v["cart_adds"]).encode()
    if (prod_id, date) in daily_sales:
        s = daily_sales[(prod_id, date)]
        row_data[b"sales:units_sold"]  = str(s["units"]).encode()
        row_data[b"sales:revenue"]     = str(round(s["revenue"],2)).encode()
        row_data[b"sales:order_count"] = str(s["orders"]).encode()
    if row_data:
        batch2.put(row_key, row_data)
        count2 += 1
batch2.send()
print(f"  done: {count2:,} product-metric rows loaded")

print("\n[4] Sample queries...")
sample_user = all_sessions[0]["user_id"]
print(f"\n  Sessions for {sample_user}:")
rows = tbl_sess.scan(row_prefix=f"{sample_user}_".encode(), limit=3,
                     columns=[b"sess:conversion_status", b"device:type"])
for rk, data in rows:
    print(f"    {rk.decode()[:35]}  "
          f"status={data.get(b'sess:conversion_status',b'?').decode()}  "
          f"device={data.get(b'device:type',b'?').decode()}")

sample_prod = list(daily_sales.keys())[0][0]
print(f"\n  Metrics for {sample_prod} in Jan 2025:")
rows2 = tbl_prod.scan(
    row_start=f"{sample_prod}_2025-01-01".encode(),
    row_stop=f"{sample_prod}_2025-02-01".encode(), limit=5)
for rk, data in rows2:
    print(f"    {rk.decode()[-20:]}  "
          f"views={data.get(b'views:view_count',b'0').decode()}  "
          f"sold={data.get(b'sales:units_sold',b'0').decode()}")

conn.close()
print("\nALL DONE! Next: run step4_spark_analysis.py")