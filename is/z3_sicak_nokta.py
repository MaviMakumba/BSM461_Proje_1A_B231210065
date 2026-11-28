# -*- coding: utf-8 -*-
"""Üye C · Z3 Sıcak Nokta (Skew) Analizi ve Tuzlama (Salting) Karşılaştırması"""
import time
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("BSM461-Z3-SicakNokta")
    .config("spark.sql.adaptive.enabled", "false")  # Ham çarpıklık etkisini net ölçmek için
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

ham = spark.read.json("s3a://ham/iot/*/*.jsonl").filter(F.col("sensor_id").isNotNull())
sensorler = spark.read.option("header", "true").csv("s3a://ham/boyut/iot/sensorler.csv")

# 1. Anahtar bazlı yoğunluk ve çarpıklık ölçümü
dagilim = ham.groupBy("sensor_id").count().orderBy(F.desc("count")).collect()
sayilar = [r["count"] for r in dagilim]
en_az, ortanca, en_cok = min(sayilar), sorted(sayilar)[len(sayilar)//2], max(sayilar)

# 2. Tuzlamasız (Standart) Kümeleme + Boyut Birleştirme
t0 = time.perf_counter()
sonuc_normal = (
    ham.groupBy("sensor_id")
    .agg(
        F.count("*").alias("olay_sayisi"),
        F.avg("sicaklik").alias("ort_sicaklik"),
        F.max("titresim").alias("maks_titresim"),
    )
    .join(sensorler.hint("MERGE"), on="sensor_id", how="left")
    .collect()
)
sure_normal = time.perf_counter() - t0

# 3. Tuzlamalı (Salting: N=10) İki Aşamalı Kümeleme + Birleştirme
TUZ_N = 10
t1 = time.perf_counter()
tuzlu_ara = (
    ham.withColumn("tuz", (F.rand(seed=461) * TUZ_N).cast("int"))
    .groupBy("sensor_id", "tuz")
    .agg(
        F.count("*").alias("ara_sayim"),
        F.sum("sicaklik").alias("ara_sicaklik_top"),
        F.count("sicaklik").alias("ara_sicaklik_adet"),
        F.max("titresim").alias("ara_maks_titresim"),
    )
)
sonuc_tuzlu = (
    tuzlu_ara.groupBy("sensor_id")
    .agg(
        F.sum("ara_sayim").alias("olay_sayisi"),
        (F.sum("ara_sicaklik_top") / F.sum("ara_sicaklik_adet")).alias("ort_sicaklik"),
        F.max("ara_maks_titresim").alias("maks_titresim"),
    )
    .join(sensorler.hint("MERGE"), on="sensor_id", how="left")
    .collect()
)
sure_tuzlu = time.perf_counter() - t1

satirlar = [
    "=== Z3 SICAK NOKTA (SKEW) VE TUZLAMA ÖLÇÜMÜ (SPARK 3.5) ===",
    f"Toplam geçerli sensör anahtarı : {len(sayilar)}",
    f"Sensör başı olay (en az / ortanca / en çok) : {en_az:,} / {ortanca:,} / {en_cok:,}",
    f"Çarpıklık oranı (en çok / ortanca)          : {en_cok / ortanca:.2f}x",
    f"En yoğun 3 sensör : {[(r['sensor_id'], r['count']) for r in dagilim[:3]]}",
    "",
    "--- Performans Karşılaştırması (3M Satır, AQE Kapalı, 16 Shuffle Bölümü) ---",
    f"Tuzlamasız (Standart groupBy + join) süresi : {sure_normal:.2f} s ({len(sonuc_normal)} satır)",
    f"Tuzlamalı  (N={TUZ_N} iki aşamalı + join) süresi : {sure_tuzlu:.2f} s ({len(sonuc_tuzlu)} satır)",
    f"Fark / Yorum : {sure_tuzlu - sure_normal:+.2f} s",
]

cikti = "\n".join(satirlar)
print(cikti)
with open("/kit/kanit/c_z3_sicak_nokta.txt", "w", encoding="utf-8") as f:
    f.write(cikti + "\n")

spark.stop()

