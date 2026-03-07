import json, os, random
from datetime import datetime, timedelta

random.seed(42)
OUTPUT_DIR = "ecommerce_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

FIRST_NAMES = ["James","Mary","John","Patricia","Robert","Jennifer","Michael",
               "Linda","William","Barbara","David","Susan","Richard","Jessica",
               "Joseph","Sarah","Thomas","Karen","Charles","Lisa","Alice","Brian",
               "Catherine","Daniel","Emily","Francis","Grace","Henry","Isabella",
               "Jack","Katherine","Liam","Mia","Noah","Olivia","Peter","Quinn",
               "Ryan","Sophia","Tyler","Uma","Victor","Wendy","Xander","Yara","Zoe"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
               "Davis","Wilson","Moore","Taylor","Anderson","Thomas","Jackson",
               "White","Harris","Martin","Thompson","Robinson","Clark","Lewis",
               "Walker","Hall","Allen","Young","King","Wright","Scott","Green",
               "Baker","Adams","Nelson","Carter","Mitchell","Roberts","Turner"]
CITIES  = ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia",
           "San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville",
           "Columbus","Charlotte","Indianapolis","Denver","Seattle","Nashville",
           "Oklahoma City","Las Vegas","Portland","Memphis","Louisville","Baltimore"]
STATES  = ["NY","CA","IL","TX","AZ","PA","TX","CA","TX","CA",
           "TX","FL","OH","NC","IN","CO","WA","TN","OK","NV","OR","TN","KY","MD"]
DOMAINS = ["gmail.com","yahoo.com","hotmail.com","outlook.com","icloud.com"]
PRODUCT_ADJECTIVES = ["Innovative","Ergonomic","Advanced","Premium","Smart",
                       "Ultimate","Professional","Essential","Modern","Classic"]
PRODUCT_NOUNS = ["Widget","System","Tool","Device","Interface","Solution",
                 "Platform","Module","Component","Framework","Kit","Suite"]

def rand_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def rand_email(name):
    parts = name.lower().replace(" ",".")
    return f"{parts}{random.randint(1,99)}@{random.choice(DOMAINS)}"

def rand_city_state():
    i = random.randint(0, len(CITIES)-1)
    return CITIES[i], STATES[i]

def rand_product_name():
    suffix = random.choice(["Pro","Plus","X","Elite","Go","Max",""])
    return f"{random.choice(PRODUCT_ADJECTIVES)} {random.choice(PRODUCT_NOUNS)} {suffix}".strip()

def fmt(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def rand_date(start, end):
    delta = int((end - start).total_seconds())
    if delta <= 0:
        return start
    return start + timedelta(seconds=random.randint(0, delta))

def rand_hex(n):
    return ''.join(random.choices('abcdef0123456789', k=n))

def rand_ip():
    return f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

START = datetime(2025, 1, 1)
END   = datetime(2025, 3, 31)

print("Generating users...", flush=True)
users = []
for i in range(500):
    name = rand_name()
    city, state = rand_city_state()
    reg  = rand_date(datetime(2024, 10, 1), datetime(2025, 1, 1))
    last = rand_date(reg, END)
    users.append({
        "user_id": f"user_{i:06d}", "name": name, "email": rand_email(name),
        "age": random.randint(18, 70), "gender": random.choice(["M","F","Other"]),
        "geo_data": {"city": city, "state": state, "country": "US"},
        "registration_date": fmt(reg), "last_active": fmt(last),
        "membership_tier": random.choice(["bronze","silver","gold","platinum"])
    })
with open(f"{OUTPUT_DIR}/users.json","w") as f:
    json.dump(users, f, indent=2)
print(f"  done: {len(users)} users saved")

print("Generating categories...", flush=True)
CAT_NAMES = ["Electronics","Clothing","Books","Home & Garden","Sports",
             "Toys","Beauty","Automotive","Food & Grocery","Jewelry",
             "Office Supplies","Pet Supplies","Music","Movies","Software",
             "Health","Baby","Tools","Travel","Art & Crafts"]
categories = []
for i, name in enumerate(CAT_NAMES):
    subs = []
    for j in range(random.randint(2,4)):
        w1 = random.choice(["Advanced","Basic","Pro","Standard","Elite"])
        w2 = random.choice(["Solutions","Products","Goods","Items","Collection"])
        subs.append({"subcategory_id": f"sub_{i:03d}_{j:02d}",
                     "name": f"{w1} {name} {w2}",
                     "profit_margin": round(random.uniform(0.10, 0.45), 2)})
    categories.append({"category_id": f"cat_{i:03d}", "name": name, "subcategories": subs})
with open(f"{OUTPUT_DIR}/categories.json","w") as f:
    json.dump(categories, f, indent=2)
print(f"  done: {len(categories)} categories saved")

print("Generating products...", flush=True)
products = []
for i in range(500):
    cat    = random.choice(categories)
    subcat = random.choice(cat["subcategories"])
    price  = round(random.uniform(5.0, 500.0), 2)
    created = rand_date(datetime(2024, 10, 1), datetime(2025, 1, 15))
    history = [{"price": round(price*random.uniform(0.85,1.15),2), "date": fmt(created)}]
    if random.random() > 0.4:
        history.append({"price": price, "date": fmt(rand_date(created, END))})
    stock = random.randint(0, 200)
    products.append({
        "product_id": f"prod_{i:05d}", "name": rand_product_name(),
        "category_id": cat["category_id"], "subcategory_id": subcat["subcategory_id"],
        "base_price": price, "current_stock": stock, "is_active": stock > 0,
        "price_history": history, "creation_date": fmt(created)
    })
with open(f"{OUTPUT_DIR}/products.json","w") as f:
    json.dump(products, f, indent=2)
print(f"  done: {len(products)} products saved")

print("Generating sessions (wait ~15 seconds)...", flush=True)
product_ids = [p["product_id"] for p in products]
REFERRERS   = ["search_engine","social_media","direct","email","paid_ad"]
DEVICES     = [
    {"type":"mobile",  "os":"iOS",     "browser":"Safari"},
    {"type":"mobile",  "os":"Android", "browser":"Chrome"},
    {"type":"desktop", "os":"Windows", "browser":"Chrome"},
    {"type":"desktop", "os":"macOS",   "browser":"Safari"},
    {"type":"tablet",  "os":"iOS",     "browser":"Safari"},
]
prod_lookup = {p["product_id"]: p for p in products}
sessions = []
for i in range(3000):
    user      = random.choice(users)
    start_t   = rand_date(START, END)
    duration  = random.randint(60, 1800)
    end_t     = start_t + timedelta(seconds=duration)
    viewed    = random.sample(product_ids, k=random.randint(1,8))
    converted = random.random() < 0.35
    device    = random.choice(DEVICES)
    page_views = [{"timestamp": fmt(start_t), "page_type":"home",
                   "product_id": None, "category_id": None,
                   "view_duration": random.randint(10,60)}]
    t = start_t
    for pid in viewed:
        t += timedelta(seconds=random.randint(20,120))
        prod = prod_lookup[pid]
        page_views.append({"timestamp": fmt(t), "page_type":"product_detail",
                           "product_id": pid, "category_id": prod["category_id"],
                           "view_duration": random.randint(30,200)})
    cart = {}
    if converted:
        t += timedelta(seconds=random.randint(10,60))
        page_views.append({"timestamp": fmt(t), "page_type":"cart",
                           "product_id": None, "category_id": None,
                           "view_duration": random.randint(20,90)})
        for pid in random.sample(viewed, k=min(len(viewed), random.randint(1,3))):
            cart[pid] = {"quantity": random.randint(1,4), "price": prod_lookup[pid]["base_price"]}
    sessions.append({
        "session_id": f"sess_{rand_hex(10)}", "user_id": user["user_id"],
        "start_time": fmt(start_t), "end_time": fmt(end_t),
        "duration_seconds": duration,
        "geo_data": {"city": user["geo_data"]["city"], "state": user["geo_data"]["state"],
                     "country":"US", "ip_address": rand_ip()},
        "device_profile": device, "viewed_products": viewed,
        "page_views": page_views, "cart_contents": cart,
        "conversion_status": "converted" if converted else "browsed",
        "referrer": random.choice(REFERRERS)
    })
    if (i+1) % 1000 == 0:
        print(f"  ... {i+1}/3000 sessions done", flush=True)
for idx, s in enumerate(range(0, len(sessions), 1000)):
    with open(f"{OUTPUT_DIR}/sessions_{idx}.json","w") as f:
        json.dump(sessions[s:s+1000], f, indent=2)
print(f"  done: {len(sessions)} sessions saved ({idx+1} files)")

print("Generating transactions...", flush=True)
converted_sessions = [s for s in sessions if s["conversion_status"] == "converted"]
transactions = []
for sess in random.sample(converted_sessions, k=min(2000, len(converted_sessions))):
    items = []; subtotal = 0.0
    for pid, ci in sess["cart_contents"].items():
        qty = ci["quantity"]; uprice = ci["price"]; sub = round(qty*uprice,2)
        subtotal += sub
        items.append({"product_id":pid,"quantity":qty,"unit_price":uprice,"subtotal":sub})
    if not items: continue
    discount = round(subtotal*random.uniform(0,0.15),2) if random.random()>0.6 else 0.0
    total    = round(subtotal - discount, 2)
    transactions.append({
        "transaction_id": f"txn_{rand_hex(12)}", "session_id": sess["session_id"],
        "user_id": sess["user_id"], "timestamp": sess["end_time"], "items": items,
        "subtotal": round(subtotal,2), "discount": discount, "total": total,
        "payment_method": random.choice(["credit_card","debit_card","paypal","apple_pay"]),
        "status": random.choice(["completed","shipped","delivered","pending","refunded"])
    })
with open(f"{OUTPUT_DIR}/transactions.json","w") as f:
    json.dump(transactions, f, indent=2)
print(f"  done: {len(transactions)} transactions saved")

print("\nALL DONE! Files created:")
for fn in sorted(os.listdir(OUTPUT_DIR)):
    size = os.path.getsize(os.path.join(OUTPUT_DIR, fn))
    print(f"  {fn}  ({size//1024} KB)")
print("\nNext: run   python step2_mongodb.py")