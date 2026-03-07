import json, os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

DATA_DIR   = "ecommerce_data"
CHARTS_DIR = "charts"
os.makedirs(CHARTS_DIR, exist_ok=True)

COLORS = ["#2563EB","#7C3AED","#059669","#DC2626","#D97706",
          "#0891B2","#BE185D","#65A30D","#9333EA","#0284C7"]
sns.set_theme(style="whitegrid")
plt.rcParams.update({"figure.dpi":150})

def load(fn):
    path = os.path.join(DATA_DIR, fn)
    if not os.path.exists(path):
        print(f"  skipping {fn} - not found"); return None
    with open(path) as f: return json.load(f)

def save(name):
    path = os.path.join(CHARTS_DIR, f"{name}.png")
    plt.savefig(path, bbox_inches="tight", dpi=150)
    plt.close()
    print(f"  saved: {path}")

print("=" * 55)
print("  STEP 5: Generating Charts")
print("=" * 55)

# Chart 1 - Top Products by Revenue
print("\n[1] Top Products by Revenue")
data = load("spark_top_products.json")
if data:
    df = pd.DataFrame(data).head(10)
    fig, ax = plt.subplots(figsize=(10,6))
    bars = ax.barh(df["name"].str[:30], df["total_revenue"], color=COLORS[:len(df)])
    ax.bar_label(bars, labels=[f"${v:,.0f}" for v in df["total_revenue"]], padding=4, fontsize=9)
    ax.set_xlabel("Total Revenue (USD)")
    ax.set_title("Top 10 Products by Revenue", fontsize=14, fontweight="bold")
    ax.invert_yaxis()
    plt.tight_layout()
    save("chart1_top_products")

# Chart 2 - Monthly Revenue Trend
print("[2] Monthly Revenue Trend")
data = load("spark_monthly_revenue.json")
if data:
    df = pd.DataFrame(data)
    fig, ax1 = plt.subplots(figsize=(10,5))
    ax2 = ax1.twinx()
    ax1.bar(df["year_month"], df["gross_revenue"], color="#2563EB", alpha=0.6, label="Gross Revenue")
    ax2.plot(df["year_month"], df["avg_order_value"], color="#DC2626",
             marker="o", linewidth=2.5, label="Avg Order Value")
    ax1.set_xlabel("Month"); ax1.set_ylabel("Revenue (USD)", color="#2563EB")
    ax2.set_ylabel("Avg Order Value (USD)", color="#DC2626")
    ax1.tick_params(axis="x", rotation=30)
    ax1.set_title("Monthly Revenue Trend", fontsize=14, fontweight="bold")
    lines1, l1 = ax1.get_legend_handles_labels()
    lines2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1+lines2, l1+l2, loc="upper left")
    plt.tight_layout()
    save("chart2_monthly_revenue")

# Chart 3 - Device Conversion
print("[3] Device Usage and Conversion")
data = load("spark_device_usage.json")
if data:
    df = pd.DataFrame(data)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12,5))
    ax1.pie(df["sessions"], labels=df["device_type"], autopct="%1.1f%%",
            colors=COLORS[:len(df)], startangle=90)
    ax1.set_title("Session Share by Device", fontsize=13, fontweight="bold")
    bars = ax2.bar(df["device_type"], df["conv_pct"], color=COLORS[:len(df)])
    ax2.bar_label(bars, labels=[f"{v:.1f}%" for v in df["conv_pct"]], padding=3)
    ax2.set_ylabel("Conversion Rate (%)")
    ax2.set_title("Conversion Rate by Device", fontsize=13, fontweight="bold")
    plt.tight_layout()
    save("chart3_device_conversion")

# Chart 4 - Referrer Performance
print("[4] Referrer Performance")
data = load("spark_referrer_performance.json")
if data:
    df = pd.DataFrame(data).sort_values("conversions", ascending=False)
    x = range(len(df)); w = 0.35
    fig, ax1 = plt.subplots(figsize=(10,5))
    ax2 = ax1.twinx()
    ax1.bar([i-w/2 for i in x], df["total_sessions"], width=w,
            color="#2563EB", alpha=0.8, label="Total Sessions")
    ax1.bar([i+w/2 for i in x], df["conversions"], width=w,
            color="#059669", alpha=0.8, label="Conversions")
    ax2.plot(x, df["conv_pct"], color="#DC2626", marker="D",
             linewidth=2, label="Conv Rate %")
    ax1.set_xticks(list(x)); ax1.set_xticklabels(df["referrer"], rotation=20)
    ax1.set_ylabel("Count"); ax2.set_ylabel("Conv Rate %", color="#DC2626")
    ax1.set_title("Traffic Source Performance", fontsize=14, fontweight="bold")
    lines1,l1=ax1.get_legend_handles_labels(); lines2,l2=ax2.get_legend_handles_labels()
    ax1.legend(lines1+lines2, l1+l2)
    plt.tight_layout()
    save("chart4_referrer_performance")

# Chart 5 - CLV Segments
print("[5] CLV Segments")
data = load("spark_clv_data.json")
if data:
    df = pd.DataFrame(data)
    seg = (df.groupby("clv_segment")
             .agg(users=("user_id","count"), avg_clv=("clv_estimate","mean"))
             .reset_index().sort_values("avg_clv", ascending=False))
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12,5))
    ax1.pie(seg["users"], labels=seg["clv_segment"], autopct="%1.1f%%",
            colors=COLORS[:len(seg)], startangle=90,
            wedgeprops={"width":0.6})
    ax1.set_title("CLV Segment Distribution", fontsize=13, fontweight="bold")
    ax2.axis("off")
    tbl = ax2.table(
        cellText=[[r["clv_segment"], f"{int(r['users']):,}", f"${r['avg_clv']:,.1f}"]
                  for _, r in seg.iterrows()],
        colLabels=["Segment","Users","Avg CLV"],
        cellLoc="center", loc="center", bbox=[0.05,0.2,0.9,0.6])
    tbl.auto_set_font_size(False); tbl.set_fontsize(11); tbl.scale(1,2)
    ax2.set_title("CLV Summary Table", fontsize=13, fontweight="bold")
    plt.tight_layout()
    save("chart5_clv_segments")

# Chart 6 - Cohort Analysis
print("[6] Cohort Analysis")
data = load("spark_cohort_analysis.json")
if data:
    df = pd.DataFrame(data).sort_values("cohort_month")
    fig, ax = plt.subplots(figsize=(10,4))
    ax.fill_between(df["cohort_month"], df["avg_ltv"], color="#2563EB", alpha=0.3)
    ax.plot(df["cohort_month"], df["avg_ltv"], color="#2563EB", marker="o", linewidth=2.5)
    for _, row in df.iterrows():
        ax.annotate(f"${row['avg_ltv']:,.0f}",
                    xy=(row["cohort_month"], row["avg_ltv"]),
                    xytext=(0,8), textcoords="offset points", ha="center", fontsize=9)
    ax.set_xlabel("Registration Month"); ax.set_ylabel("Average LTV (USD)")
    ax.set_title("Average LTV by Registration Cohort", fontsize=14, fontweight="bold")
    ax.tick_params(axis="x", rotation=30)
    plt.tight_layout()
    save("chart6_cohort_ltv")

# Chart 7 - Conversion Funnel
print("[7] Conversion Funnel")
session_files = [f for f in os.listdir(DATA_DIR)
                 if f.startswith("sessions_") and f.endswith(".json")]
if session_files:
    all_s = []
    for fn in session_files:
        with open(os.path.join(DATA_DIR, fn)) as f:
            all_s.extend(json.load(f))
    total      = len(all_s)
    w_product  = sum(1 for s in all_s if s.get("viewed_products"))
    w_cart     = sum(1 for s in all_s if s.get("cart_contents"))
    purchased  = sum(1 for s in all_s if s.get("conversion_status")=="converted")
    stages  = ["All Sessions","Viewed Product","Added to Cart","Purchased"]
    counts  = [total, w_product, w_cart, purchased]
    colors2 = ["#2563EB","#7C3AED","#D97706","#059669"]
    fig, ax = plt.subplots(figsize=(9,5))
    bars = ax.bar(stages, counts, color=colors2, edgecolor="white", linewidth=1.5)
    for bar, cnt in zip(bars, counts):
        pct = cnt/total*100
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+20,
                f"{cnt:,}\n({pct:.1f}%)", ha="center", va="bottom", fontsize=10)
    ax.set_ylabel("Count"); ax.set_ylim(0, max(counts)*1.2)
    ax.set_title("E-Commerce Conversion Funnel", fontsize=14, fontweight="bold")
    plt.tight_layout()
    save("chart7_conversion_funnel")

# Chart 8 - Price Tier Revenue
print("[8] Price Tier Revenue")
data = load("spark_top_products.json")
prods = load("products.json")
if data and prods:
    prod_price = {p["product_id"]: p["base_price"] for p in prods}
    df = pd.DataFrame(data)
    df["base_price"] = df["product_id"].map(lambda pid: prod_price.get(pid, 0))
    df["tier"] = df["base_price"].apply(
        lambda p: "Premium(>=200)" if p>=200 else ("Mid(50-199)" if p>=50 else "Budget(<50)"))
    tier_agg = df.groupby("tier").agg(revenue=("total_revenue","sum")).reset_index()
    fig, ax = plt.subplots(figsize=(8,5))
    bars = ax.bar(tier_agg["tier"], tier_agg["revenue"], color=COLORS[:len(tier_agg)])
    ax.bar_label(bars, labels=[f"${v:,.0f}" for v in tier_agg["revenue"]], padding=4)
    ax.set_ylabel("Total Revenue (USD)")
    ax.set_title("Revenue by Product Price Tier", fontsize=14, fontweight="bold")
    plt.tight_layout()
    save("chart8_price_tier")

print(f"\nALL DONE! Open the 'charts/' folder to see your 8 PNG charts.")
print("Charts are ready to copy into your report.")
