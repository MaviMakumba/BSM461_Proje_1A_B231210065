# -*- coding: utf-8 -*-
"""B3 / K-09e — Enjeksiyon deneyinin Kafka verisini FARKLI uyarı kurallarıyla çevrimdışı yeniden değerlendirir.

Çalıştırma (deney bitince, konu silinmeden): docker compose exec spark spark-submit is/olcum/b3_k09e_karsilastir.py <etiket>
Neden çevrimdışı: Aynı 5,7 milyon mesaj, aynı 4 atak → kurallar birebir aynı veride karşılaştırılır (canlıda her kural için ayrı
45 dk'lık deney gerekirdi ve her seferinde rastgele veri farklı olurdu).
Yöntem: Her mesaj Kafka'ya girdiği sırayla işlenir; kural her mesajdan sonra değerlendirilir (canlıda 2 s'lik yığın sonunda
değerlendirilir → canlı süre ≈ çevrimdışı süre + yığın payı; R0 için iki ölçüm karşılaştırılarak bu pay ölçülür).
Pencereye atama olay zamanıyla (olay_zamani), A1'deki gibi. Sıçrama = K-09a (herhangi bir ölçü |x − medyan| > 5 × dss).

Kurallar:
  R0  mevcut (K-09b/c): 10 dk sabit pencere, oran > %1 VE ≥ 12 sıçrama
  R1  kısa sabit pencere: 2 dk, oran > %1 VE ≥ 12 sıçrama
  R2  kayan pencere: 10 dk, 1 dk adım (her olay 10 pencerede), oran > %1 VE ≥ 12 sıçrama
  R3  son 60 s sayımı: sensörün son 60 saniyedeki sıçrama sayısı ≥ 12 (oran yok)
  R4  R3'ün Spark akışında yazılabilen hâli: 1 dk kayan pencere, 10 s adım, ≥ 12 sıçrama
Uyarı = kuralın (sensör, pencere) için İLK doğru olduğu an (R3: sayım 12'ye yükseldiği an) — A1 de yalnız yeni ⚠ basar.
Yanlış alarm birimi: (sensör, 10 dk takvim penceresi) — o pencerede saldırı dışında en az bir uyarı başladıysa 1.
"""
import csv
import statistics as st
import sys

from pyspark.sql import SparkSession, Window, functions as F

et = sys.argv[1]
spark = (SparkSession.builder.appName("k09e").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "64").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
SEMA = "olay_zamani STRING, sensor_id STRING, bolge STRING, sicaklik DOUBLE, nem DOUBLE, titresim DOUBLE"
k = (spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "sensorler")
     .option("startingOffsets", "earliest").load()
     .select(F.from_json(F.col("value").cast("string"), SEMA).alias("o"), F.col("timestamp").cast("double").alias("kz"))
     .select("o.sensor_id", "o.sicaklik", "o.nem", "o.titresim", F.to_timestamp("o.olay_zamani").cast("double").alias("z"), "kz")
     .where(F.col("sensor_id").isNotNull() & F.col("z").isNotNull()))
sensor = spark.read.option("header", True).csv("s3a://ham/boyut/iot/sensorler.csv").select("sensor_id", "bolge")
esik = (spark.read.parquet("s3a://islenmis/_stub/iot_esik.parquet").groupBy("bolge", "saat")
        .pivot("olcu", ["sicaklik", "nem", "titresim"]).agg(F.first("medyan").alias("m"), F.first("dss").alias("d")))


def asti(o):
    return F.coalesce(F.abs(F.col(o) - F.col(f"{o}_m")) > 5 * F.col(f"{o}_d"), F.lit(False))


k = (k.join(F.broadcast(sensor), "sensor_id").withColumn("saat", F.hour(F.col("z").cast("timestamp")))
     .join(F.broadcast(esik), ["bolge", "saat"])
     .where(F.col("sicaklik").isNotNull() | F.col("nem").isNotNull() | F.col("titresim").isNotNull())
     .select("sensor_id", "z", "kz", (asti("sicaklik") | asti("nem") | asti("titresim")).cast("int").alias("an"))
     .repartition(64, "sensor_id").cache())
print(f"mesaj: {k.count():,}")


def sabit(df, W):            # sabit pencere: her mesajdan sonra pencerenin o ana kadarki ölçüm/sıçrama sayısı
    w = Window.partitionBy("sensor_id", "p").orderBy("kz").rowsBetween(Window.unboundedPreceding, 0)
    return (df.withColumn("p", F.floor(F.col("z") / W) * W)
            .withColumn("n", F.count("*").over(w)).withColumn("a", F.sum("an").over(w)))


def kayan(df, W, S, gruplar=5):  # kayan pencere: her olay W/S pencereye girer (bellek için sensörler gruplar hâlinde)
    adim, parcalar = int(W // S), []
    for g in range(gruplar):
        d = df.where(F.abs(F.hash("sensor_id")) % gruplar == g)
        d = d.withColumn("i", F.explode(F.sequence(F.lit(0), F.lit(adim - 1))))
        d = d.withColumn("p", F.floor(F.col("z") / S) * S - F.col("i") * S).drop("i")
        w = Window.partitionBy("sensor_id", "p").orderBy("kz").rowsBetween(Window.unboundedPreceding, 0)
        d = d.withColumn("n", F.count("*").over(w)).withColumn("a", F.sum("an").over(w))
        parcalar += yield_ilk(d)
    return parcalar


def yield_ilk(d, kosul=None):    # UYARI = (sensör, pencere) için kuralın İLK doğru olduğu an (A1 de yalnız yeni ⚠ basar)
    kosul = kosul if kosul is not None else KOSUL
    return d.where(kosul).groupBy("sensor_id", "p").agg(F.min("kz").alias("kz")).select("sensor_id", "kz").collect()


def sabit(df, W):            # sabit pencere: her mesajdan sonra pencerenin o ana kadarki ölçüm/sıçrama sayısı
    w = Window.partitionBy("sensor_id", "p").orderBy("kz").rowsBetween(Window.unboundedPreceding, 0)
    return yield_ilk(df.withColumn("p", F.floor(F.col("z") / W) * W)
                     .withColumn("n", F.count("*").over(w)).withColumn("a", F.sum("an").over(w)))


def son60(df):               # UYARI = sayım 12'nin altından 12'ye çıktığı an (yükselen kenar)
    w = Window.partitionBy("sensor_id").orderBy("kz_ms")
    d = (df.withColumn("kz_ms", (F.col("kz") * 1000).cast("long"))
         .withColumn("a", F.sum("an").over(w.rangeBetween(-60000, 0))))
    d = d.withColumn("once", F.lag("a", 1, 0).over(w))
    return d.where((F.col("a") >= 12) & (F.col("once") < 12)).select("sensor_id", "kz").collect()


KOSUL = (F.col("a") >= 12) & (100 * F.col("a") / F.col("n") > 1.0)
SAYI12 = F.col("a") >= 12
kurallar = {
    "R0 10 dk sabit, >%1 ve ≥12": lambda: sabit(k, 600),
    "R1 2 dk sabit, >%1 ve ≥12": lambda: sabit(k, 120),
    "R2 10 dk kayan (1 dk adım), >%1 ve ≥12": lambda: kayan(k, 600, 60),
    "R3 son 60 s'de ≥12 sıçrama": lambda: son60(k),
    "R4 1 dk kayan (10 s adım), ≥12": lambda: kayan(k, 60, 10, gruplar=2),
    "R4b 1 dk kayan (10 s adım), >%1 ve ≥15": lambda: kayan(k, 60, 10, gruplar=2),
    "R4c 1 dk kayan (10 s adım), >%1 ve ≥20": lambda: kayan(k, 60, 10, gruplar=2),
}
if len(sys.argv) > 2:                         # yalnız adı verilen kurallar (ör. "R4b,R4c")
    kurallar = {a: f for a, f in kurallar.items() if a.split()[0] in sys.argv[2].split(",")}

ataklar = [r for r in csv.DictReader(open("/kit/kanit/b3/enjeksiyon-ataklar.csv", encoding="utf-8")) if r["etiket"] == et]
aralik = []
for at in ataklar:
    b, s = (int(x) for x in at["sensorler"].split("-"))
    for i in range(b, s + 1):
        aralik.append((f"sensor-{i:03d}", float(at["ilk_gonderim"]) - 1, float(at["atak_bit"]) + 60, int(at["konum_s"]), float(at["atak_bas"])))
top_pencere = k.select("sensor_id", F.floor(F.col("z") / 600).alias("p")).distinct().count()

out = [f"# K-09e çevrimdışı karşılaştırma · etiket {et} · aynı Kafka verisi, 4 atak × 10 sensör · "
       f"yanlış alarm birimi: (sensör, 10 dk pencere), toplam {top_pencere} sensör-pencere", "",
       "kural | tespit 40/40 | medyan s | en çok s | konum 5/300/570/595 en çok s | yanlış alarm (sensör-pencere) | yanlış alarm veren sensörler"]
for ad, df in kurallar.items():
    if ad.startswith("R4 "):
        KOSUL = SAYI12
    elif ad.startswith("R4b"):
        KOSUL = (F.col("a") >= 15) & (100 * F.col("a") / F.col("n") > 1.0)
    elif ad.startswith("R4c"):
        KOSUL = (F.col("a") >= 20) & (100 * F.col("a") / F.col("n") > 1.0)
    isaret = df()
    gecikme, konum = [], {}
    for sid, a0, a1, kon, bas in aralik:
        t = [r["kz"] for r in isaret if r["sensor_id"] == sid and a0 <= r["kz"] <= a1]
        if t:
            d = round(min(t) - bas, 2)
            gecikme.append(d)
            konum.setdefault(kon, []).append(d)
    yanlis = {(r["sensor_id"], int(r["kz"] // 600)) for r in isaret
              if not any(sid == r["sensor_id"] and a0 <= r["kz"] <= a1 for sid, a0, a1, _, _ in aralik)}
    out.append(f"{ad} | {len(gecikme)}/{len(aralik)} | {st.median(gecikme) if gecikme else '-'} | {max(gecikme) if gecikme else '-'} | "
               + "/".join(str(max(konum.get(c, [float('nan')]))) for c in (5, 300, 570, 595))
               + f" | {len(yanlis)} | {sorted({s for s, _ in yanlis})}")
    print(out[-1], flush=True)
open("/kit/kanit/b3/k09e-karsilastirma.txt", "a" if len(sys.argv) > 2 else "w", encoding="utf-8").write("\n".join(out) + "\n")
spark.stop()
