"""Demo fitur Iceberg: query, snapshots, time travel, schema evolution, compaction."""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("iceberg-demo")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
T = "local.db.orders"

def section(title):
    print(f"\n{'=' * 8} {title} {'=' * 8}")

section("1. Agregasi dasar")
spark.sql(f"SELECT city, count(*) AS orders, round(avg(amount),1) AS avg_amount "
          f"FROM {T} GROUP BY city ORDER BY orders DESC").show()

section("2. Snapshots (setiap commit streaming)")
snaps = spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {T}.snapshots ORDER BY committed_at")
snaps.show(truncate=False)
rows = snaps.collect()

section("3. Time travel: snapshot pertama vs sekarang")
if len(rows) >= 2:
    first = rows[0]["snapshot_id"]
    old = spark.sql(f"SELECT count(*) AS n FROM {T} VERSION AS OF {first}").first()["n"]
    now = spark.sql(f"SELECT count(*) AS n FROM {T}").first()["n"]
    print(f"Snapshot {first}: {old} baris | Sekarang: {now} baris")
else:
    print("Belum cukup snapshot, tunggu beberapa detik lalu jalankan ulang.")

section("4. Schema evolution (tanpa rewrite data)")
cols = [c.name for c in spark.table(T).schema.fields]
if "channel" not in cols:
    spark.sql(f"ALTER TABLE {T} ADD COLUMN channel STRING")
    print("Kolom 'channel' ditambahkan.")
spark.sql(f"SELECT id, city, amount, channel FROM {T} LIMIT 5").show()

section("5. Compaction (gabung small files)")
before = spark.sql(f"SELECT count(*) AS n FROM {T}.files").first()["n"]
spark.sql("CALL local.system.rewrite_data_files(table => 'db.orders')").show(truncate=False)
after = spark.sql(f"SELECT count(*) AS n FROM {T}.files").first()["n"]
print(f"Jumlah data file (metadata): {before} -> {after} (file lama baru terhapus setelah snapshot yang mereferensikannya kedaluwarsa)")

section("6. Expire snapshot lama")
spark.sql("""CALL local.system.expire_snapshots(
    table => 'db.orders',
    older_than => TIMESTAMP '2100-01-01 00:00:00',
    retain_last => 3)""").show(truncate=False)
