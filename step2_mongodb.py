import json, os
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_analytics"]
DATA_DIR = "ecommerce_data"

print("=" * 55)
print("  STEP 2: MongoDB Loading & Analytics")
print("=" * 55)

def load_json(filename):
    with open(os.path.join(DATA_DIR, filename)) as f:
        return json.load(f)

def load_collection(name, docs):
    col = db[name]
    col.drop()
    col.insert_many(docs, ordered=False)
    print(f"  done: {col.count_documents({}):,} docs in '{name}'")
    return col

print("\n[1] Loading collections...")
col_users = load_collection("users",        load_json("users.json"))
col_cats  = load_collection("categories",   load_json("categories.json"))
col_prods = load_collection("products",     load_json("products.json"))
col_txns  = load_collection("transactions", load_json("transactions.json"))

all_sessions = []
for fn in sorted(os.listdir(DATA_DIR)):
    if fn.startswith("sessions_") and fn.endswith(".json"):
        all_sessions.extend(load_json(fn))
col_sess = load_collection("sessions", all_sessions)

col_users.create_index("user_id")
col_prods.create_index("category_id")
col_txns.create_index("user_id")
col_txns.create_index("timestamp")
col_sess.create_index("user_id")
print("  done: indexes created")

print("\n[2] Aggregation 1: Top 10 Products by Revenue")
pipeline1 = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "total_revenue": {"$sum": "$items.subtotal"},
        "units_sold":    {"$sum": "$items.quantity"},
        "order_count":   {"$sum": 1}
    }},
    {"$lookup": {
        "from": "products", "localField": "_id",
        "foreignField": "product_id", "as": "info"
    }},
    {"$unwind": {"path": "$info", "preserveNullAndEmptyArrays": True}},
    {"$project": {
        "product_id": "$_id",
        "product_name": {"$ifNull": ["$info.name", "Unknown"]},
        "category_id": "$info.category_id",
        "total_revenue": {"$round": ["$total_revenue", 2]},
        "units_sold": 1, "order_count": 1
    }},
    {"$sort": {"total_revenue": -1}},
    {"$limit": 10}
]
results = list(col_txns.aggregate(pipeline1))
print(f"  {'Rank':<5} {'Product':<35} {'Revenue':>12} {'Units':>7}")
print("  " + "-" * 62)
for rank, r in enumerate(results, 1):
    print(f"  {rank:<5} {r['product_name'][:34]:<35} ${r['total_revenue']:>10,.2f} {r['units_sold']:>7}")

print("\n[3] Aggregation 2: Monthly Revenue by Category")
pipeline2 = [
    {"$match": {"status": {"$in": ["completed","shipped","delivered"]}}},
    {"$unwind": "$items"},
    {"$lookup": {"from": "products", "localField": "items.product_id",
                 "foreignField": "product_id", "as": "prod"}},
    {"$unwind": {"path": "$prod", "preserveNullAndEmptyArrays": True}},
    {"$lookup": {"from": "categories", "localField": "prod.category_id",
                 "foreignField": "category_id", "as": "cat"}},
    {"$unwind": {"path": "$cat", "preserveNullAndEmptyArrays": True}},
    {"$group": {
        "_id": {
            "month":    {"$substr": ["$timestamp", 0, 7]},
            "category": {"$ifNull": ["$cat.name", "Unknown"]}
        },
        "revenue": {"$sum": "$items.subtotal"},
        "orders":  {"$sum": 1}
    }},
    {"$sort": {"_id.month": 1, "revenue": -1}},
    {"$project": {
        "month": "$_id.month", "category": "$_id.category",
        "revenue": {"$round": ["$revenue", 2]}, "orders": 1
    }}
]
results2 = list(col_txns.aggregate(pipeline2))
print(f"  {'Month':<10} {'Category':<22} {'Revenue':>12} {'Orders':>7}")
print("  " + "-" * 55)
for r in results2[:15]:
    print(f"  {r['month']:<10} {r['category'][:21]:<22} ${r['revenue']:>10,.2f} {r['orders']:>7}")

print("\n[4] Aggregation 3: User Spending Segments")
pipeline3 = [
    {"$match": {"status": {"$in": ["completed","shipped","delivered"]}}},
    {"$group": {
        "_id": "$user_id",
        "total_spent": {"$sum": "$total"},
        "order_count": {"$sum": 1}
    }},
    {"$addFields": {
        "segment": {
            "$switch": {
                "branches": [
                    {"case": {"$gte": ["$total_spent", 1000]}, "then": "High (>=1000)"},
                    {"case": {"$gte": ["$total_spent", 300]},  "then": "Mid (300-999)"},
                    {"case": {"$gte": ["$total_spent", 50]},   "then": "Low (50-299)"},
                ],
                "default": "Micro (<50)"
            }
        }
    }},
    {"$group": {
        "_id": "$segment",
        "user_count": {"$sum": 1},
        "avg_spent":  {"$avg": "$total_spent"},
        "avg_orders": {"$avg": "$order_count"}
    }},
    {"$sort": {"avg_spent": -1}}
]
results3 = list(col_txns.aggregate(pipeline3))
print(f"  {'Segment':<20} {'Users':>7} {'Avg Spent':>12} {'Avg Orders':>12}")
print("  " + "-" * 55)
for r in results3:
    print(f"  {r['_id']:<20} {r['user_count']:>7} ${r['avg_spent']:>10,.2f} {r['avg_orders']:>12.1f}")

print("\n[5] Aggregation 4: Session Conversion Funnel")
pipeline4 = [
    {"$group": {"_id": "$conversion_status", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
results4 = list(col_sess.aggregate(pipeline4))
total = sum(r["count"] for r in results4)
print(f"  {'Status':<20} {'Count':>8} {'Pct':>8}")
print("  " + "-" * 40)
for r in results4:
    pct = r["count"] / total * 100 if total > 0 else 0
    print(f"  {r['_id']:<20} {r['count']:>8,} {pct:>7.1f}%")

print("\n[6] Saving results for charts...")
os.makedirs(DATA_DIR, exist_ok=True)
for data, name in [(results, "agg_top_products"), (results2, "agg_monthly_revenue")]:
    clean = [{k: v for k, v in d.items() if k != "_id"} for d in data]
    with open(f"{DATA_DIR}/{name}.json", "w") as f:
        json.dump(clean, f, indent=2, default=str)
    print(f"  saved: {name}.json")

client.close()
print("\nALL DONE! Next: set up Docker HBase then run step3_hbase.py")