"""
TechTrove E-Commerce — Data Integration Pipeline
Data Engineer Lab: รวมข้อมูล Orders (CSV) + Customers (CRM CSV) + Products (Excel)
                    + Payments (nested JSON) -> Dimension/Fact tables + Data Quality Report

รันได้ตั้งแต่ต้นจนจบด้วย:  python techtrove_pipeline.py
Output ทั้งหมดจะถูกเขียนไปที่โฟลเดอร์ ./output
"""

from pathlib import Path
import json
import re
import pandas as pd
import numpy as np

DATA = Path(__file__).parent / "data"
OUTPUT = Path(__file__).parent / "output"
OUTPUT.mkdir(exist_ok=True)

pd.set_option("display.width", 140)

# เก็บ log ของทุกแถว/เหตุการณ์ที่ถูกคัดออกหรือแก้ไข เพื่อสร้าง Data Quality Report ที่ตรวจสอบย้อนกลับได้
dq_log = []


def log_dq(stage, check, count, reason):
    """บันทึกเหตุการณ์คุณภาพข้อมูลหนึ่งรายการลง dq_log"""
    dq_log.append({"stage": stage, "check": check, "count": int(count), "reason": reason})
    print(f"  [DQ] {stage:<12} | {check:<28} | count={count:<5} | {reason}")


def section(title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


# ---------------------------------------------------------------------------
# TODO 1: Extract ข้อมูลจาก CSV, Excel และ JSON  +  Profile ก่อนแก้ไขใดๆ (5.1)
# ---------------------------------------------------------------------------
section("STEP 1 — EXTRACT & PROFILE (RAW, BEFORE ANY CLEANING)")

orders_jan_raw = pd.read_csv(DATA / "orders_2026_01.csv")
orders_feb_raw = pd.read_csv(DATA / "orders_2026_02.csv")
customers_raw = pd.read_csv(DATA / "customers_crm.csv")
products_raw = pd.read_excel(DATA / "product_master.xlsx")

with open(DATA / "payments.json", encoding="utf-8") as f:
    payments_json = json.load(f)
# payments.json เป็น nested JSON: {"payment_id","order_id","payment":{"method","status"},"paid_at"}
payments_raw = pd.json_normalize(payments_json)


def profile(name, df):
    print(f"\n--- {name} ---")
    print("shape       :", df.shape)
    print("columns     :", list(df.columns))
    print("dtypes      :\n", df.dtypes.to_string())
    print("missing     :\n", df.isna().sum().to_string())
    print("duplicates  :", df.duplicated().sum(), "(full-row) |",
          "order_id dup:", df["order_id"].duplicated().sum() if "order_id" in df.columns else "n/a")
    print("sample:\n", df.head(3).to_string())


raw_frames = {
    "orders_2026_01 (raw)": orders_jan_raw,
    "orders_2026_02 (raw)": orders_feb_raw,
    "customers_crm (raw)": customers_raw,
    "product_master (raw)": products_raw,
    "payments (raw, normalized)": payments_raw,
}
for name, df in raw_frames.items():
    profile(name, df)

raw_row_counts_before = {
    "orders_2026_01": len(orders_jan_raw),
    "orders_2026_02": len(orders_feb_raw),
    "customers_crm": len(customers_raw),
    "product_master": len(products_raw),
    "payments": len(payments_raw),
}

# ---------------------------------------------------------------------------
# TODO 2: ทำ schema alignment ของไฟล์ orders สองเดือน แล้ว concat (5.2)
# ---------------------------------------------------------------------------
section("STEP 2 — COMBINE ORDERS (SCHEMA ALIGNMENT + CONCAT)")

orders_jan = orders_jan_raw.copy()
orders_feb = orders_feb_raw.copy()

# ก.พ. ใช้ชื่อคอลัมน์ต่างจาก ม.ค. -> ปรับให้ตรงกับ ม.ค.
#   ordered_at   -> order_date
#   qty          -> quantity
#   discount_pct -> discount (เป็นรูปแบบ '5%' string -> ต้องแปลงเป็น float 0.05)
orders_feb = orders_feb.rename(columns={"ordered_at": "order_date", "qty": "quantity"})
orders_feb["discount"] = (
    orders_feb["discount_pct"].astype(str).str.rstrip("%").astype(float) / 100.0
)
orders_feb = orders_feb.drop(columns=["discount_pct"])

# วันที่ของ ม.ค. เป็น 'YYYY-MM-DD HH:MM:SS', ของ ก.พ. เป็น 'DD/MM/YYYY HH:MM' -> parse แยกก่อนรวม
orders_jan["order_date"] = pd.to_datetime(orders_jan["order_date"], format="%Y-%m-%d %H:%M:%S")
orders_feb["order_date"] = pd.to_datetime(orders_feb["order_date"], format="%d/%m/%Y %H:%M")

# จัดลำดับคอลัมน์ให้ตรงกันทุกไฟล์ก่อน concat
col_order = ["order_id", "order_date", "customer_id", "product_id", "quantity", "unit_price", "discount", "channel"]
orders_jan = orders_jan[col_order]
orders_feb = orders_feb[col_order]

orders_combined = pd.concat([orders_jan, orders_feb], ignore_index=True)
rows_after_concat = len(orders_combined)
print(f"orders_2026_01: {len(orders_jan)} rows | orders_2026_02: {len(orders_feb)} rows")
print(f"orders_combined (after concat, before dedupe): {rows_after_concat} rows")

# ---------------------------------------------------------------------------
# TODO 3: Clean/standardize/deduplicate และสร้าง data quality report (5.3)
# ---------------------------------------------------------------------------
section("STEP 3 — TRANSFORM: TYPES, STANDARDIZATION, DEDUPLICATION")

orders = orders_combined.copy()

# แปลงชนิดข้อมูลให้ชัดเจน
orders["quantity"] = pd.to_numeric(orders["quantity"], errors="coerce").astype("Int64")
orders["unit_price"] = pd.to_numeric(orders["unit_price"], errors="coerce")
orders["discount"] = pd.to_numeric(orders["discount"], errors="coerce")
orders["customer_id"] = orders["customer_id"].astype(str).str.strip()
orders["product_id"] = orders["product_id"].astype(str).str.strip()

# 3.1 order_id ซ้ำ -> เก็บแถวล่าสุดตามลำดับที่ปรากฏ (keep='last')
dup_order_id_mask = orders.duplicated(subset="order_id", keep=False)
n_dup_order_id = orders.duplicated(subset="order_id", keep="last").sum()
if n_dup_order_id:
    log_dq("dedupe", "duplicate order_id", n_dup_order_id,
           "order_id ซ้ำ - เก็บเฉพาะแถวล่าสุดตามลำดับที่ปรากฏ (keep='last')")
orders = orders.drop_duplicates(subset="order_id", keep="last").reset_index(drop=True)
rows_after_dedupe = len(orders)
print(f"rows after concat: {rows_after_concat} -> rows after order_id dedupe: {rows_after_dedupe}")

# --- customers: lower-case/trim email, standardize province, dedupe exact-duplicate customer rows ---
customers = customers_raw.copy()
customers["customer_id"] = customers["customer_id"].astype(str).str.strip()

n_missing_email = customers["email"].isna().sum()
if n_missing_email:
    log_dq("clean", "missing email", n_missing_email, "email เป็นค่าว่าง - คงแถวไว้ (ไม่กระทบการคำนวณยอดขาย) แต่บันทึกไว้")
customers["email"] = customers["email"].astype(str).str.strip().str.lower()
customers.loc[customers["email"].isin(["nan", "none", ""]), "email"] = pd.NA

province_map = {
    "ชลบุรี": "ชลบุรี", "chonburi": "ชลบุรี",
    "ขอนแก่น": "ขอนแก่น", "ขอนเเก่น": "ขอนแก่น",
    "กรุงเทพมหานคร": "กรุงเทพมหานคร", "bangkok": "กรุงเทพมหานคร", "กทม.": "กรุงเทพมหานคร",
    "ระยอง": "ระยอง", "rayong": "ระยอง",
    "ภูเก็ต": "ภูเก็ต", "phuket": "ภูเก็ต",
    "เชียงใหม่": "เชียงใหม่", "chiang mai": "เชียงใหม่",
}
raw_province = customers["province"].astype(str).str.strip()
customers["province"] = raw_province.str.lower().map(province_map)
n_unmapped_province = customers["province"].isna().sum()
if n_unmapped_province:
    log_dq("standardize", "unmapped province", n_unmapped_province,
           f"ค่า province ไม่อยู่ใน mapping ที่กำหนด: {sorted(raw_province[customers['province'].isna()].unique())}")
    customers["province"] = customers["province"].fillna(raw_province)  # กันไม่ให้หายไปเฉยๆ

n_dup_customer = customers.duplicated(subset="customer_id", keep="first").sum()
if n_dup_customer:
    log_dq("dedupe", "duplicate customer_id", n_dup_customer,
           "customer_id ซ้ำ (ข้อมูลซ้ำทุกคอลัมน์) - เก็บแถวแรกไว้")
customers = customers.drop_duplicates(subset="customer_id", keep="first").reset_index(drop=True)

# --- products: trim ids, keep as-is otherwise ---
products = products_raw.copy()
products["product_id"] = products["product_id"].astype(str).str.strip()
n_dup_product = products.duplicated(subset="product_id", keep="first").sum()
if n_dup_product:
    log_dq("dedupe", "duplicate product_id", n_dup_product, "product_id ซ้ำ - เก็บแถวแรกไว้")
products = products.drop_duplicates(subset="product_id", keep="first").reset_index(drop=True)

# --- payments: normalize nested JSON columns, dedupe exact-duplicate payment rows ---
payments = payments_raw.copy()
payments = payments.rename(columns={"payment.method": "payment_method", "payment.status": "payment_status"})
payments["order_id"] = payments["order_id"].astype(str).str.strip()
payments["paid_at"] = pd.to_datetime(payments["paid_at"], errors="coerce")

n_dup_payment_order = payments.duplicated(subset="order_id", keep="first").sum()
if n_dup_payment_order:
    log_dq("dedupe", "duplicate payment order_id", n_dup_payment_order,
           "order_id ซ้ำในไฟล์ payments (แถวซ้ำทุกคอลัมน์) - เก็บแถวแรกไว้ เพื่อให้ merge เป็น 1:1 ได้")
payments = payments.drop_duplicates(subset="order_id", keep="first").reset_index(drop=True)

print(f"customers: {len(customers_raw)} -> {len(customers)} rows after dedupe")
print(f"products : {len(products_raw)} -> {len(products)} rows after dedupe")
print(f"payments : {len(payments_raw)} -> {len(payments)} rows after dedupe")

# ---------------------------------------------------------------------------
# TODO 4: Enrich ด้วย customer, product และ payment master (5.4 Integrate)
# ---------------------------------------------------------------------------
section("STEP 4 — INTEGRATE: MERGE + VALIDATE CARDINALITY / UNMATCHED KEYS")

# merge กับ customer master (many orders : 1 customer)
merged = orders.merge(
    customers[["customer_id", "full_name", "email", "province"]],
    on="customer_id", how="left", validate="m:1", indicator="cust_match"
)
n_unmatched_cust = (merged["cust_match"] == "left_only").sum()
if n_unmatched_cust:
    log_dq("integrate", "unmatched customer_id", n_unmatched_cust,
           "customer_id ในคำสั่งซื้อไม่พบใน customers_crm (Master Data)")
merged = merged.drop(columns="cust_match")

# merge กับ product master (many orders : 1 product)
merged = merged.merge(
    products[["product_id", "product_name", "category", "standard_price", "active_flag"]],
    on="product_id", how="left", validate="m:1", indicator="prod_match"
)
n_unmatched_prod = (merged["prod_match"] == "left_only").sum()
if n_unmatched_prod:
    log_dq("integrate", "unmatched product_id", n_unmatched_prod,
           "product_id ในคำสั่งซื้อไม่พบใน product_master (Master Data)")
merged = merged.drop(columns="prod_match")

# merge กับ payments (1 order : 1 payment event)
merged = merged.merge(
    payments[["order_id", "payment_method", "payment_status", "paid_at"]],
    on="order_id", how="left", validate="1:1", indicator="pay_match"
)
n_unmatched_pay = (merged["pay_match"] == "left_only").sum()
if n_unmatched_pay:
    log_dq("integrate", "unmatched payment order_id", n_unmatched_pay,
           "order_id ในคำสั่งซื้อไม่พบ payment event ใน payments.json")
merged = merged.drop(columns="pay_match")

print(f"merged shape: {merged.shape}")
print(f"unmatched customer_id : {n_unmatched_cust}")
print(f"unmatched product_id  : {n_unmatched_prod}")
print(f"unmatched payment     : {n_unmatched_pay}")

# ---------------------------------------------------------------------------
# TODO 5: Validate business rules ก่อนคำนวณยอดขาย (กติกาทางธุรกิจ ข้อ 4)
# ---------------------------------------------------------------------------
section("STEP 5 — VALIDATE BUSINESS RULES")

def validate_data(df):
    """
    ตรวจสอบ uniqueness, referential integrity และค่าที่อยู่นอกช่วงตามกติกาทางธุรกิจ
    คืนค่า dict ของผลตรวจสอบ; raise AssertionError หากพบปัญหาที่ยอมรับไม่ได้ (เช่น order_id ซ้ำหลัง dedupe)
    """
    results = {}
    results["order_id_unique"] = df["order_id"].is_unique
    assert results["order_id_unique"], "order_id ยังมีค่าซ้ำอยู่หลัง dedupe!"

    results["n_quantity_invalid"] = int((df["quantity"] <= 0).sum())
    results["n_unit_price_invalid"] = int(((df["unit_price"] <= 0) | df["unit_price"].isna()).sum())
    results["n_discount_out_of_range"] = int((~df["discount"].between(0, 1)).sum())
    results["n_customer_unmatched"] = int(df["full_name"].isna().sum())
    results["n_product_unmatched"] = int(df["product_name"].isna().sum())
    return results


validation_results = validate_data(merged)
print("validate_data() results:")
for k, v in validation_results.items():
    print(f"  {k}: {v}")

# ธงความถูกต้องของแต่ละแถวตามกติกาทางธุรกิจ
valid_quantity = merged["quantity"] > 0
valid_unit_price = merged["unit_price"].notna() & (merged["unit_price"] > 0)
valid_discount = merged["discount"].between(0, 1)
valid_customer = merged["full_name"].notna()
valid_product = merged["product_name"].notna()
is_paid = merged["payment_status"] == "PAID"

n_bad_quantity = (~valid_quantity).sum()
n_bad_unit_price = (~valid_unit_price).sum()
n_bad_discount = (~valid_discount).sum()

if n_bad_quantity:
    log_dq("validate", "quantity <= 0", n_bad_quantity, "quantity ต้อง > 0 ตามกติกาทางธุรกิจ - ตัดออกจาก fact_sales")
if n_bad_unit_price:
    log_dq("validate", "unit_price invalid", n_bad_unit_price,
           "unit_price ต้อง > 0 และไม่ใช่ค่าว่าง ตามกติกาทางธุรกิจ - ตัดออกจาก fact_sales")
if n_bad_discount:
    log_dq("validate", "discount out of [0,1]", n_bad_discount,
           "discount ต้องอยู่ในช่วง 0-1 ตามกติกาทางธุรกิจ - ตัดออกจาก fact_sales")

n_not_paid = (~is_paid).sum()
log_dq("validate", "payment_status != PAID", n_not_paid,
       "นับเป็นยอดขายเฉพาะเมื่อ payment.status = PAID เท่านั้น (รวม FAILED/REFUNDED/ไม่พบ payment)")

# แถวที่ "ใช้ได้จริง" สำหรับคำนวณยอดขาย = ผ่านทุกกติกา + จับคู่ master ได้ + จ่ายเงินสำเร็จ
is_valid_row = (
    valid_quantity & valid_unit_price & valid_discount & valid_customer & valid_product & is_paid
)
n_valid_sales = int(is_valid_row.sum())
print(f"\nแถวที่ผ่านกติกาทั้งหมดและนับเป็นยอดขายได้จริง: {n_valid_sales} / {len(merged)}")

# ---------------------------------------------------------------------------
# net_sales = quantity * unit_price * (1 - discount)
# ---------------------------------------------------------------------------
merged["net_sales"] = merged["quantity"] * merged["unit_price"] * (1 - merged["discount"])
merged["is_valid_sale"] = is_valid_row

# ---------------------------------------------------------------------------
# TODO 6: Load dim_customer.csv, dim_product.csv และ fact_sales.csv (5.5)
# ---------------------------------------------------------------------------
section("STEP 6 — LOAD: DIMENSION / FACT TABLES + DATA QUALITY REPORT")

dim_customer = customers[["customer_id", "full_name", "email", "province", "signup_date"]].copy()
dim_product = products[["product_id", "product_name", "category", "standard_price", "active_flag"]].copy()

fact_sales = merged.loc[
    is_valid_row,
    ["order_id", "order_date", "customer_id", "product_id", "quantity", "unit_price",
     "discount", "net_sales", "channel", "payment_method", "payment_status", "paid_at",
     "province", "category"],
].reset_index(drop=True)

dim_customer.to_csv(OUTPUT / "dim_customer.csv", index=False, encoding="utf-8-sig")
dim_product.to_csv(OUTPUT / "dim_product.csv", index=False, encoding="utf-8-sig")
fact_sales.to_csv(OUTPUT / "fact_sales.csv", index=False, encoding="utf-8-sig")

print(f"dim_customer.csv : {len(dim_customer)} rows")
print(f"dim_product.csv  : {len(dim_product)} rows")
print(f"fact_sales.csv   : {len(fact_sales)} rows, net_sales total = {fact_sales['net_sales'].sum():,.2f}")

dq_report = pd.DataFrame(dq_log)
dq_report.to_csv(OUTPUT / "data_quality_report.csv", index=False, encoding="utf-8-sig")
print(f"\ndata_quality_report.csv : {len(dq_report)} entries")
print(dq_report.to_string(index=False))

# ---------------------------------------------------------------------------
# TODO 7: สร้าง summary_by_province.csv และ summary_by_category.csv (5.6 Analyze)
# ---------------------------------------------------------------------------
section("STEP 7 — ANALYZE: SUMMARY BY PROVINCE / CATEGORY")

summary_by_province = (
    fact_sales.groupby("province", dropna=False)
    .agg(n_transactions=("order_id", "count"), net_sales=("net_sales", "sum"))
    .sort_values("net_sales", ascending=False)
    .reset_index()
)
summary_by_category = (
    fact_sales.groupby("category", dropna=False)
    .agg(n_transactions=("order_id", "count"), net_sales=("net_sales", "sum"))
    .sort_values("net_sales", ascending=False)
    .reset_index()
)

summary_by_province.to_csv(OUTPUT / "summary_by_province.csv", index=False, encoding="utf-8-sig")
summary_by_category.to_csv(OUTPUT / "summary_by_category.csv", index=False, encoding="utf-8-sig")

print("\nsummary_by_province.csv:")
print(summary_by_province.to_string(index=False))
print("\nsummary_by_category.csv:")
print(summary_by_category.to_string(index=False))

# ---------------------------------------------------------------------------
# CHALLENGE (+2): Data-quality funnel  raw -> deduplicated -> matched -> paid sales
# ---------------------------------------------------------------------------
section("CHALLENGE — DATA QUALITY FUNNEL")

funnel = {
    "raw (orders concat)": rows_after_concat,
    "deduplicated (order_id)": rows_after_dedupe,
    "matched customer+product": int((valid_customer & valid_product).sum()),
    "valid business rules": int((valid_quantity & valid_unit_price & valid_discount &
                                  valid_customer & valid_product).sum()),
    "paid sales (final)": n_valid_sales,
}
funnel_df = pd.DataFrame(list(funnel.items()), columns=["stage", "row_count"])
funnel_df.to_csv(OUTPUT / "data_quality_funnel.csv", index=False, encoding="utf-8-sig")
print(funnel_df.to_string(index=False))

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(funnel_df["stage"], funnel_df["row_count"], color="#3b6fa0")
    for i, v in enumerate(funnel_df["row_count"]):
        ax.text(v, i, f" {v:,}", va="center")
    ax.invert_yaxis()
    ax.set_xlabel("Row count")
    ax.set_title("Data Quality Funnel: Raw -> Deduplicated -> Matched -> Paid Sales")
    plt.tight_layout()
    fig.savefig(OUTPUT / "data_quality_funnel.png", dpi=150)
    print("saved chart -> output/data_quality_funnel.png")
except ImportError:
    print("matplotlib ไม่พร้อมใช้งาน - ข้ามการสร้างกราฟ (มี data_quality_funnel.csv แทน)")

# ---------------------------------------------------------------------------
# คำถามวิเคราะห์ 6 ข้อ (พร้อมอ้างอิงตัวเลขจากผลรันจริง)
# ---------------------------------------------------------------------------
section("ANALYSIS QUESTIONS — คำตอบอ้างอิงจากผลรันจริง")

top_province = summary_by_province.iloc[0]
top_category = summary_by_category.iloc[0]

answers = f"""
Q1) หลังรวมไฟล์ orders มีจำนวนแถวเท่าใด และเหลือกี่แถวหลังลบ duplicate?
    -> หลัง concat: {rows_after_concat} แถว | หลังลบ duplicate (order_id ซ้ำ, keep='last'): {rows_after_dedupe} แถว

Q2) มีแถวที่ customer_id หรือ product_id ไม่พบใน Master Data อย่างละกี่แถว?
    -> customer_id ไม่พบใน Master Data: {n_unmatched_cust} แถว
    -> product_id ไม่พบใน Master Data:  {n_unmatched_prod} แถว

Q3) มียอดขายที่ใช้ได้จริงกี่ธุรกรรม และยอดขายสุทธิรวมเท่าใด?
    -> ธุรกรรมที่ใช้ได้จริง (ผ่านทุกกติกา + payment_status = PAID): {n_valid_sales} ธุรกรรม
    -> ยอดขายสุทธิรวม (net_sales): {fact_sales['net_sales'].sum():,.2f} บาท

Q4) จังหวัดใดมียอดขายสุทธิสูงสุด?
    -> {top_province['province']} (net_sales = {top_province['net_sales']:,.2f} บาท, {int(top_province['n_transactions'])} ธุรกรรม)

Q5) หมวดสินค้าใดมียอดขายสุทธิสูงสุด?
    -> {top_category['category']} (net_sales = {top_category['net_sales']:,.2f} บาท, {int(top_category['n_transactions'])} ธุรกรรม)

Q6) หากสลับลำดับ merge ก่อน cleaning ผลลัพธ์หรือความเชื่อมั่นของข้อมูลเปลี่ยนอย่างไร?
    -> หาก merge ก่อนทำความสะอาด (dedupe/standardize) จะเกิดปัญหาหลายอย่าง:
       1. แถว order_id ที่ยังซ้ำอยู่จะถูก merge กับ master data ซ้ำซ้อนตามไปด้วย ทำให้ยอดขาย
          ถูกนับซ้ำ (double-counting) และตัวเลข net_sales รวมสูงเกินจริง
       2. เนื่องจากยัง validate='m:1'/'1:1' ไม่ผ่าน (เพราะ payments/customers ยังมีแถวซ้ำ)
          คำสั่ง merge(validate=...) จะ raise MergeError ทันที ทำให้ pipeline รันไม่สำเร็จ
       3. province ที่ยังไม่ standardize (เช่น 'Bangkok' vs 'กรุงเทพมหานคร') จะถูกนับแยกกลุ่มกัน
          ทำให้ summary_by_province ผิดเพี้ยน กระจายยอดขายจังหวัดเดียวออกเป็นหลายแถว
       4. โดยสรุป: การ clean ก่อน merge ช่วยให้ cardinality ของความสัมพันธ์ (m:1, 1:1) ถูกต้อง
          และทำให้ผลลัพธ์ที่ได้มีความน่าเชื่อถือ ตรวจสอบย้อนกลับได้ ตรงตามที่ deteministic
          ส่วนการ merge ก่อน clean จะให้ผลลัพธ์ที่ไม่แน่นอนและมีความเสี่ยงสูงที่จะนับข้อมูลผิดพลาด
"""
print(answers)

with open(OUTPUT / "analysis_answers.txt", "w", encoding="utf-8") as f:
    f.write(answers)

# ---------------------------------------------------------------------------
# สรุปคุณภาพข้อมูล ก่อน/หลัง Integration
# ---------------------------------------------------------------------------
section("DATA QUALITY SUMMARY — BEFORE vs AFTER")

before_after = pd.DataFrame([
    {"metric": "orders rows (raw, 2 files summed)",
     "before": raw_row_counts_before["orders_2026_01"] + raw_row_counts_before["orders_2026_02"],
     "after": rows_after_dedupe},
    {"metric": "customers rows", "before": raw_row_counts_before["customers_crm"], "after": len(dim_customer)},
    {"metric": "products rows", "before": raw_row_counts_before["product_master"], "after": len(dim_product)},
    {"metric": "payments rows", "before": raw_row_counts_before["payments"], "after": len(payments)},
    {"metric": "duplicate order_id (orders)", "before": n_dup_order_id, "after": 0},
    {"metric": "unmatched customer_id", "before": n_unmatched_cust, "after": 0},
    {"metric": "unmatched product_id", "before": n_unmatched_prod, "after": 0},
    {"metric": "invalid quantity (<=0)", "before": n_bad_quantity, "after": 0},
    {"metric": "invalid/missing unit_price", "before": n_bad_unit_price, "after": 0},
    {"metric": "valid, paid transactions (final fact_sales)", "before": np.nan, "after": n_valid_sales},
])
before_after.to_csv(OUTPUT / "data_quality_before_after.csv", index=False, encoding="utf-8-sig")
print(before_after.to_string(index=False))

section("PIPELINE COMPLETE ✔")
print(f"ไฟล์ผลลัพธ์ทั้งหมดถูกบันทึกไว้ที่: {OUTPUT.resolve()}")
for p in sorted(OUTPUT.glob("*")):
    print(" -", p.name)
