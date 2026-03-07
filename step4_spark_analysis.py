import json, os, pandas as pd

D = "C:/Users/Ahishakiye/ulk_project/ecommerce_data"

print("="*55)
print("  STEP 4: Analytics")
print("="*55)

def load(f):
    with open(D + "/" + f) as x:
        return json.load(x)

print("\n[1] Loading data...")
users    = load("users.json")
products = load("products.json")
txns     = load("transactions.json")
sessions = load("sessions_0.json") + load("sessions_1.json") + load("sessions_2.json")

df_users = pd.DataFrame(users)

txn_rows = []
for t in txns:
    for item in t.get("items", []):
        txn_rows.append({
            "transaction_id": t["transaction_id"],
            "user_id":        t["user_id"],
            "year_month":     t["timestamp"][:7],
            "status":         t.get("status", "unknown"),
            "total":          t.get("total", 0.0),
            "product_id":     item["product_id"],
            "quantity":       item["quantity"],
            "item_subtotal":  item["subtotal"]
        })

df_items = pd.DataFrame(txn_rows)
df_txns  = pd.DataFrame([{
    "transaction_id": t["transaction_id"],
    "user_id":        t["user_id"],
    "year_month":     t["timestamp"][:7],
    "status":         t.get("status", "unknown"),
    "total":          t.get("total", 0.0)
} for t in txns])

df_prods = pd.DataFrame([{
    "product_id": p["product_id"],
    "name":       p["name"],
    "base_price": p["base_price"],
    "price_tier": "premium" if p["base_price"] >= 200 else ("mid" if p["base_price"] >= 50 else "budget")
} for p in products])

df_sess = pd.DataFrame([{
    "session_id":        s["session_id"],
    "user_id":           s["user_id"],
    "duration_seconds":  s.get("duration_seconds", 0),
    "conversion_status": s.get("conversion_status", "unknown"),
    "referrer":          s.get("referrer", "direct"),
    "device_type":       s.get("device_profile", {}).get("type", "unknown")
} for s in sessions])

print("  users=" + str(len(users)) + "  products=" + str(len(products)))
print("  transactions=" + str(len(txns)) + "  sessions=" + str(len(sessions)))

GOOD = ["completed", "shipped", "delivered"]
good_items = df_items[df_items["status"].isin(GOOD)]
good_txns  = df_txns[df_txns["status"].isin(GOOD)]

print("\n[3] Top 10 Products by Revenue")
q1 = good_items.groupby("product_id").agg(
    units_sold=("quantity", "sum"),
    total_revenue=("item_subtotal", "sum"),
    num_orders=("transaction_id", "nunique")
).reset_index().merge(
    df_prods[["product_id", "name", "price_tier"]], on="product_id", how="left"
).sort_values("total_revenue", ascending=False).head(10)
q1["total_revenue"] = q1["total_revenue"].round(2)
print(q1[["product_id", "name", "price_tier", "units_sold", "total_revenue"]].to_string(index=False))

print("\n[4] Monthly Revenue Trend")
q2 = good_txns.groupby("year_month").agg(
    num_transactions=("transaction_id", "nunique"),
    gross_revenue=("total", "sum"),
    avg_order_value=("total", "mean")
).reset_index().sort_values("year_month")
q2["gross_revenue"]   = q2["gross_revenue"].round(2)
q2["avg_order_value"] = q2["avg_order_value"].round(2)
print(q2.to_string(index=False))

print("\n[5] Device Conversion Rates")
q3 = df_sess.groupby("device_type").agg(
    sessions=("session_id", "count"),
    avg_min=("duration_seconds", "mean"),
    conversions=("conversion_status", lambda x: (x == "converted").sum())
).reset_index()
q3["avg_min"]  = (q3["avg_min"] / 60).round(1)
q3["conv_pct"] = (q3["conversions"] / q3["sessions"] * 100).round(2)
print(q3.sort_values("sessions", ascending=False).to_string(index=False))

print("\n[6] Referrer Performance")
q4 = df_sess.groupby("referrer").agg(
    total_sessions=("session_id", "count"),
    conversions=("conversion_status", lambda x: (x == "converted").sum())
).reset_index()
q4["conv_pct"] = (q4["conversions"] / q4["total_sessions"] * 100).round(2)
print(q4.sort_values("conversions", ascending=False).to_string(index=False))

print("\n[7] Co-Purchase Recommendations")
g2 = df_items[df_items["status"].isin(GOOD)][["transaction_id", "product_id"]]
mg = g2.merge(g2, on="transaction_id")
pr = mg[mg["product_id_x"] < mg["product_id_y"]]
cp = pr.groupby(["product_id_x", "product_id_y"]).size().reset_index(name="co_purchase_count")
cp = cp[cp["co_purchase_count"] >= 2].sort_values("co_purchase_count", ascending=False).head(10)
print(cp.to_string(index=False))

print("\n[8] Cohort Analysis")
df_users["cohort_month"] = df_users["registration_date"].str[:7]
user_spend = good_txns.groupby("user_id").agg(
    ltv=("total", "sum"), orders=("transaction_id", "count")
).reset_index()
cohort = df_users.merge(user_spend, on="user_id", how="left").fillna({"ltv": 0.0, "orders": 0})
cohort = cohort.groupby("cohort_month").agg(
    users=("user_id", "count"),
    avg_ltv=("ltv", "mean"),
    avg_orders=("orders", "mean")
).reset_index()
cohort["avg_ltv"]    = cohort["avg_ltv"].round(2)
cohort["avg_orders"] = cohort["avg_orders"].round(2)
print(cohort.sort_values("cohort_month").to_string(index=False))

print("\n[9] Customer Lifetime Value")
ss = df_sess.groupby("user_id").agg(
    total_sessions=("session_id", "count"),
    conversions=("conversion_status", lambda x: (x == "converted").sum())
).reset_index()
clv = df_users[["user_id", "membership_tier"]].merge(
    user_spend, on="user_id", how="left"
).merge(ss, on="user_id", how="left").fillna({"ltv": 0.0, "orders": 0, "total_sessions": 0})
clv["avg_order"]    = clv.apply(lambda r: r["ltv"] / r["orders"] if r["orders"] > 0 else 0, axis=1)
clv["clv_estimate"] = (clv["avg_order"] * clv["orders"] * 1.5).round(2)
clv["clv_segment"]  = pd.cut(
    clv["clv_estimate"], bins=[-1, 50, 300, 1000, 999999],
    labels=["Low", "Medium", "High", "VIP"])
sm = clv.groupby("clv_segment").agg(
    users=("user_id", "count"),
    avg_clv=("clv_estimate", "mean"),
    avg_sessions=("total_sessions", "mean")
).reset_index()
sm["avg_clv"]      = sm["avg_clv"].round(2)
sm["avg_sessions"] = sm["avg_sessions"].round(1)
print(sm.sort_values("avg_clv", ascending=False).to_string(index=False))

print("\n[10] Saving results...")
def save(df, name):
    df.to_json(D + "/spark_" + name + ".json", orient="records", indent=2)
    print("  saved: spark_" + name + ".json (" + str(len(df)) + " rows)")

save(q1, "top_products")
save(q2, "monthly_revenue")
save(q3, "device_usage")
save(q4, "referrer_performance")
save(cp, "copurchase_pairs")
save(cohort, "cohort_analysis")
save(clv[["user_id", "membership_tier", "clv_estimate", "clv_segment", "orders", "total_sessions"]].rename(columns={"orders": "order_count"}), "clv_data")

print("\nALL DONE! Next: run step5_visualizations.py")