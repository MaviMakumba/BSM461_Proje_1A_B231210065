# -*- coding: utf-8 -*-
"""Üye C · Z10 Veri Temizleme, Karantina ve Zenginleştirme Hattı (Bronz -> Gümüş)"""
import time
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("BSM461-Z10-Temizleme")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

t0 = time.perf_counter()

ham = spark.read.json("s3a://ham/iot/*/*.jsonl")
sensorler = spark.read.option("header", "true").csv("s3a://ham/boyut/iot/sensorler.csv")

toplam_ham = ham.count()

# 1. Tekrar (duplicate) kayıtları ayıkla
tekil = ham.dropDuplicates()
tekrar_silinen = toplam_ham - tekil.count()

# 2. Geçerlilik kuralları (eksik alanlar ve fiziksel sınırlar)
gecerli_kosul = (
    F.col("sensor_id").isNotNull()
    & F.col("bolge").isNotNull()
    & F.col("sicaklik").isNotNull()
    & F.col("nem").isNotNull()
    & F.col("titresim").isNotNull()
    & F.col("sicaklik").between(-50.0, 100.0)
    & F.col("nem").between(0.0, 100.0)
    & (F.col("titresim") >= 0.0)
    & F.to_timestamp("olay_zamani").isNotNull()
)

karantina = tekil.filter(~gecerli_kosul)
karantina_sayisi = karantina.count()

# 3. Temiz veriyi boyut tablosuyla (Broadcast Join) zenginleştir ve uç değer bayrağı ekle
temiz = (
    tekil.filter(gecerli_kosul)
    .withColumn("olay_ts", F.to_timestamp("olay_zamani")).withColumn("tarih", F.to_date("olay_ts"))
    .withColumn(
        "anomali_bayragi",
        F.when((F.col("sicaklik") > 50.0) | (F.col("titresim") > 2.5), F.lit(True)).otherwise(F.lit(False))
    )
    .join(F.broadcast(sensorler.select("sensor_id", "model", "kurulum_yili")), on="sensor_id", how="left")
)
temiz_sayisi = temiz.count()
anomali_sayisi = temiz.filter(F.col("anomali_bayragi")).count()

# 4. S3 'islenmis' kovasına Parquet olarak yaz
temiz.write.mode("overwrite").partitionBy("tarih").parquet("s3a://islenmis/iot/temiz")
karantina.write.mode("overwrite").parquet("s3a://islenmis/iot/karantina")

sure = time.perf_counter() - t0

satirlar = [
    "=== Z10 VERİ TEMİZLEME VE ZENGİNLEŞTİRME ÖZETİ (SPARK 3.5) ===",
    f"1. Ham giriş kaydı (s3://ham/iot)          : {toplam_ham:,}",
    f"2. Silinen tam tekrar (duplicate)          : -{tekrar_silinen:,} (%{tekrar_silinen/toplam_ham*100:.3f})",
    f"3. Karantinaya alınan eksik/hatalı kayıt   : -{karantina_sayisi:,} (%{karantina_sayisi/toplam_ham*100:.3f})",
    f"4. Temiz + zenginleştirilmiş (s3://islenmis): {temiz_sayisi:,} (%{temiz_sayisi/toplam_ham*100:.3f})",
    f"   └─ İçindeki uç değer (anomali) işaretli : {anomali_sayisi:,} (%{anomali_sayisi/temiz_sayisi*100:.3f})",
    f"Toplam çalışma süresi (okuma+temiz+yazma)  : {sure:.2f} s",
]

cikti = "\n".join(satirlar)
print(cikti)
with open("/kit/kanit/c_z10_temizleme_ozeti.txt", "w", encoding="utf-8") as f:
    f.write(cikti + "\n")

spark.stop()

