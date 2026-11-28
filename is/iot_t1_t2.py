# -*- coding: utf-8 -*-
"""Rol A — Z5 Toplu Analiz Tabloları: iot_t1 ve iot_t2 (Delta Lake).

Çalıştırma:
  docker compose exec -T spark spark-submit is/iot_t1_t2.py

Girdiler (K-15 kanonik veri):
  - s3a://ham/iot/tarih=*/*.jsonl
  - s3a://ham/boyut/iot/sensorler.csv
  - s3a://tablolar/iot_esik (K-09a, 504 satır, Delta)

Çıktılar (Delta Lake):
  - s3a://tablolar/iot_t1 : (tarih, saat, bolge, ort_sicaklik, ort_nem, ort_titresim, olcum_sayisi)
  - s3a://tablolar/iot_t2 : (model, kurulum_yili, sensor_sayisi, gerceklesen_olcum, beklenen_olcum,
                             anomali_sayisi, anomali_orani, veri_kaybi_orani)
"""
import sys
import time
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

HAM_YOL = "s3a://ham/iot/tarih=*/*.jsonl"
SENSOR_YOL = "s3a://ham/boyut/iot/sensorler.csv"
ESIK_YOL = "s3a://tablolar/iot_esik"
T1_YOL = "s3a://tablolar/iot_t1"
T2_YOL = "s3a://tablolar/iot_t2"

HAM_SEMA = T.StructType([
    T.StructField("olay_zamani", T.StringType(), True),
    T.StructField("sensor_id", T.StringType(), True),
    T.StructField("bolge", T.StringType(), True),
    T.StructField("sicaklik", T.DoubleType(), True),
    T.StructField("nem", T.DoubleType(), True),
    T.StructField("titresim", T.DoubleType(), True),
])


def main() -> int:
    spark = (
        SparkSession.builder
        .appName("rol-a-iot-t1-t2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    temiz_df = None
    try:
        t0 = time.perf_counter()

        # 1. Ham veriyi şema ile oku, K-15 kuralına göre tüm sütunlarda tekrar ayıkla
        ham_df = spark.read.schema(HAM_SEMA).json(HAM_YOL)
        tekil_df = ham_df.dropDuplicates()

        sensor_df = (
            spark.read.option("header", "true").csv(SENSOR_YOL)
            .select(
                F.col("sensor_id").cast("string").alias("sensor_id"),
                F.col("bolge").cast("string").alias("bolge"),
                F.col("model").cast("string").alias("model"),
                F.col("kurulum_yili").cast("int").alias("kurulum_yili"),
            )
        )
        sensor_sayisi = sensor_df.count()
        if sensor_sayisi == 0:
            raise RuntimeError("sensorler.csv bos veya okunamadi.")

        # Bölge bilgisi sensorler.csv'den alınır; olay_zamani UTC tarih ve saate çevrilir
        temiz_df = (
            tekil_df
            .drop("bolge")
            .withColumn("olay_ts", F.to_timestamp(F.col("olay_zamani")))
            .filter(F.col("olay_ts").isNotNull())
            .withColumn("tarih", F.to_date(F.col("olay_ts")))
            .withColumn("saat", F.hour(F.col("olay_ts")).cast("int"))
            .join(F.broadcast(sensor_df), on="sensor_id", how="inner")
            .select("tarih", "saat", "sensor_id", "bolge", "model", "kurulum_yili", "sicaklik", "nem", "titresim")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        temiz_satir = temiz_df.count()
        if temiz_satir == 0:
            raise RuntimeError("Temizlenmis veri seti bos.")

        # 2. T1: Bölge bazında gün içi (tarih, saat) ortalama ölçümler
        t1_df = (
            temiz_df
            .groupBy("tarih", "saat", "bolge")
            .agg(
                F.round(F.avg("sicaklik"), 4).cast("double").alias("ort_sicaklik"),
                F.round(F.avg("nem"), 4).cast("double").alias("ort_nem"),
                F.round(F.avg("titresim"), 4).cast("double").alias("ort_titresim"),
                F.count("*").cast("long").alias("olcum_sayisi"),
            )
            .orderBy("tarih", "saat", "bolge")
        )

        (
            t1_df
            .coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(T1_YOL)
        )

        # 3. T2: K-09a eşik tablosunu (7 bölge x 24 saat = 168 satır) geniş biçime aç
        esik_df = spark.read.format("delta").load(ESIK_YOL)
        esik_genis = (
            esik_df
            .groupBy("bolge", "saat")
            .agg(
                F.max(F.when(F.col("olcu") == "sicaklik", F.col("medyan"))).alias("med_sicaklik"),
                F.max(F.when(F.col("olcu") == "sicaklik", F.col("dss"))).alias("dss_sicaklik"),
                F.max(F.when(F.col("olcu") == "nem", F.col("medyan"))).alias("med_nem"),
                F.max(F.when(F.col("olcu") == "nem", F.col("dss"))).alias("dss_nem"),
                F.max(F.when(F.col("olcu") == "titresim", F.col("medyan"))).alias("med_titresim"),
                F.max(F.when(F.col("olcu") == "titresim", F.col("dss"))).alias("dss_titresim"),
            )
        )
        esik_grup_sayisi = esik_genis.count()
        if esik_grup_sayisi != 168:
            raise RuntimeError(f"iot_esik bolge x saat sayisi 168 olmali, bulunan: {esik_grup_sayisi}")

        anomali_kosul = (
            (F.col("sicaklik").isNotNull() & (F.abs(F.col("sicaklik") - F.col("med_sicaklik")) > F.lit(5.0) * F.col("dss_sicaklik")))
            | (F.col("nem").isNotNull() & (F.abs(F.col("nem") - F.col("med_nem")) > F.lit(5.0) * F.col("dss_nem")))
            | (F.col("titresim").isNotNull() & (F.abs(F.col("titresim") - F.col("med_titresim")) > F.lit(5.0) * F.col("dss_titresim")))
        )

        sensor_istatistik = (
            temiz_df
            .join(F.broadcast(esik_genis), on=["bolge", "saat"], how="inner")
            .groupBy("sensor_id")
            .agg(
                F.count("*").cast("long").alias("olcum_sayisi"),
                F.sum(F.when(anomali_kosul, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("anomali_sayisi"),
            )
        )

        sensor_tam = (
            sensor_df
            .join(sensor_istatistik, on="sensor_id", how="left")
            .fillna({"olcum_sayisi": 0, "anomali_sayisi": 0})
        )

        # Beklenen ölçüm tabanı: 200 sensörün medyan ölçüm sayısı (sağlıklı sensör hızı)
        ref_olcum = float(
            sensor_tam.agg(F.expr("percentile(olcum_sayisi, 0.5)").alias("med")).first()["med"]
        )
        if ref_olcum <= 0:
            raise RuntimeError(f"Gecersiz referans olcum medyani: {ref_olcum}")

        t2_df = (
            sensor_tam
            .withColumn(
                "eksik_olcum",
                F.greatest(F.lit(0.0), F.lit(ref_olcum) - F.col("olcum_sayisi").cast("double")),
            )
            .groupBy("model", "kurulum_yili")
            .agg(
                F.count("*").cast("int").alias("sensor_sayisi"),
                F.sum("olcum_sayisi").cast("long").alias("gerceklesen_olcum"),
                F.round(F.count("*") * F.lit(ref_olcum), 1).cast("double").alias("beklenen_olcum"),
                F.sum("anomali_sayisi").cast("long").alias("anomali_sayisi"),
                F.when(
                    F.sum("olcum_sayisi") > 0,
                    F.round(F.lit(100.0) * F.sum("anomali_sayisi") / F.sum("olcum_sayisi"), 4),
                ).otherwise(F.lit(0.0)).cast("double").alias("anomali_orani"),
                F.round(
                    F.lit(100.0) * F.sum("eksik_olcum") / (F.count("*") * F.lit(ref_olcum)),
                    4,
                ).cast("double").alias("veri_kaybi_orani"),
            )
            .orderBy("model", "kurulum_yili")
        )

        (
            t2_df
            .coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(T2_YOL)
        )

        sure = time.perf_counter() - t0

        t1_oku = spark.read.format("delta").load(T1_YOL)
        t2_oku = spark.read.format("delta").load(T2_YOL)
        t1_say = t1_oku.count()
        t2_say = t2_oku.count()

        print(f"✓ Temiz girdi: {temiz_satir:,} satir | Referans sensor medyani: {ref_olcum:.1f} olcum/14 gun | Sure: {sure:.2f} sn")
        print(f"✓ {T1_YOL} yazildi | satir: {t1_say}")
        t1_oku.printSchema()
        t1_oku.show(5, truncate=False)

        print(f"✓ {T2_YOL} yazildi | satir: {t2_say}")
        t2_oku.printSchema()
        print("En yuksek anomali oranli 10 (model, kurulum_yili) grubu:")
        t2_oku.orderBy(F.desc("anomali_orani")).show(10, truncate=False)
        print("En yuksek veri kaybi oranli 10 (model, kurulum_yili) grubu:")
        t2_oku.orderBy(F.desc("veri_kaybi_orani")).show(10, truncate=False)

        return 0
    except Exception as exc:
        print(f"[HATA] iot_t1_t2 uretimi basarisiz: {exc}", file=sys.stderr)
        return 1
    finally:
        if temiz_df is not None:
            temiz_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())