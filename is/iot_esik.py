# -*- coding: utf-8 -*-
"""Rol A — K-09a ortak eşik tablosu (s3a://tablolar/iot_esik, Delta Lake).

Çalıştırma:
  docker compose exec spark spark-submit is/iot_esik.py
  docker compose exec spark spark-submit is/iot_esik.py --dogrula-stub

Sözleşme (K-09a):
  (bolge STRING, saat INT, olcu STRING, medyan DOUBLE, dss DOUBLE) · 7 bölge × 24 saat × 3 ölçü = 504 satır
  - Tekrar ayıklama: ham JSON'un TÜM sütunları üzerinde dropDuplicates()
  - Bölge: sensorler.csv'den (sensor_id inner join)
  - Saat: olay_zamani UTC saati (0–23)
  - Medyan ve MAD: kesin percentile(x, 0.5) (percentile_approx kullanılmaz); dss = 1.4826 × MAD
"""
import argparse
import sys
import time
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

HAM_YOL = "s3a://ham/iot/tarih=*/*.jsonl"
SENSOR_YOL = "s3a://ham/boyut/iot/sensorler.csv"
HEDEF_YOL = "s3a://tablolar/iot_esik"
STUB_YOL = "s3a://islenmis/_stub/iot_esik.parquet"

HAM_SEMA = T.StructType([
    T.StructField("olay_zamani", T.StringType(), True),
    T.StructField("sensor_id", T.StringType(), True),
    T.StructField("bolge", T.StringType(), True),
    T.StructField("sicaklik", T.DoubleType(), True),
    T.StructField("nem", T.DoubleType(), True),
    T.StructField("titresim", T.DoubleType(), True),
])


def stub_ile_karsilastir(spark: SparkSession, delta_yol: str, stub_yol: str) -> None:
    a_df = spark.read.format("delta").load(delta_yol).alias("a")
    b_df = spark.read.parquet(stub_yol).alias("b")

    a_say = a_df.count()
    b_say = b_df.count()
    if a_say != b_say or a_say != 504:
        raise RuntimeError(f"Satir sayisi uyusmazligi: Delta={a_say}, Stub={b_say}, Beklenen=504")

    birlesik = a_df.join(b_df, on=["bolge", "saat", "olcu"], how="full_outer")
    eksik = birlesik.filter(F.col("a.medyan").isNull() | F.col("b.medyan").isNull()).count()
    if eksik > 0:
        raise RuntimeError(f"Anahtar uyusmazligi tespit edildi: {eksik} satir eslesmedi.")

    fark = birlesik.agg(
        F.max(F.abs(F.col("a.medyan") - F.col("b.medyan"))).alias("max_medyan_fark"),
        F.max(F.abs(F.col("a.dss") - F.col("b.dss"))).alias("max_dss_fark"),
    ).first()

    max_med = float(fark["max_medyan_fark"] or 0.0)
    max_dss = float(fark["max_dss_fark"] or 0.0)
    print(f"Stub karsilastirma | satir: {a_say} | max |medyan farki|: {max_med:.12g} | max |dss farki|: {max_dss:.12g}")

    if max_med > 1e-9 or max_dss > 1e-9:
        raise RuntimeError(f"Sayisal sapma tolerans disinda: medyan={max_med}, dss={max_dss}")
    print("✓ birebir aynı (Delta iot_esik == DuckDB stub)")


def main() -> int:
    ap = argparse.ArgumentParser(description="K-09a iot_esik Delta tablosunu uretir.")
    ap.add_argument("--dogrula-stub", action="store_true", help="Uretim sonrasi stub Parquet ile karsilastir.")
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .appName("rol-a-iot-esik")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    uzun_df = None
    try:
        t0 = time.perf_counter()

        ham_df = spark.read.schema(HAM_SEMA).json(HAM_YOL)
        tekilsiz_df = ham_df.dropDuplicates()

        sensor_df = (
            spark.read.option("header", "true").csv(SENSOR_YOL)
            .select(
                F.col("sensor_id").cast("string").alias("sensor_id"),
                F.col("bolge").cast("string").alias("bolge"),
            )
        )

        hazir_df = (
            tekilsiz_df
            .drop("bolge")
            .withColumn("saat", F.hour(F.to_timestamp(F.col("olay_zamani"))).cast("int"))
            .filter(F.col("saat").isNotNull())
            .join(F.broadcast(sensor_df), on="sensor_id", how="inner")
        )

        uzun_df = (
            hazir_df
            .select(
                F.col("bolge"),
                F.col("saat"),
                F.expr("stack(3, 'sicaklik', sicaklik, 'nem', nem, 'titresim', titresim) as (olcu, deger)"),
            )
            .filter(F.col("deger").isNotNull())
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        medyan_df = (
            uzun_df
            .groupBy("bolge", "saat", "olcu")
            .agg(F.expr("percentile(deger, 0.5)").cast("double").alias("medyan"))
        )

        esik_df = (
            uzun_df
            .join(F.broadcast(medyan_df), on=["bolge", "saat", "olcu"], how="inner")
            .groupBy("bolge", "saat", "olcu", "medyan")
            .agg(
                (F.lit(1.4826) * F.expr("percentile(abs(deger - medyan), 0.5)"))
                .cast("double")
                .alias("dss")
            )
            .select(
                F.col("bolge").cast("string"),
                F.col("saat").cast("int"),
                F.col("olcu").cast("string"),
                F.col("medyan").cast("double"),
                F.col("dss").cast("double"),
            )
            .orderBy("bolge", "saat", "olcu")
        )

        (
            esik_df
            .coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(HEDEF_YOL)
        )

        sure = time.perf_counter() - t0

        okunan = spark.read.format("delta").load(HEDEF_YOL)
        satir_sayisi = okunan.count()
        print(f"✓ {HEDEF_YOL} yazildi | satir: {satir_sayisi} | sure: {sure:.2f} sn")
        okunan.printSchema()
        (
            okunan.groupBy("olcu")
            .agg(
                F.round(F.min("dss"), 3).alias("dss_min"),
                F.round(F.max("dss"), 3).alias("dss_max"),
                F.count("*").alias("satir"),
            )
            .orderBy("olcu")
            .show(truncate=False)
        )

        if satir_sayisi != 504:
            raise RuntimeError(f"Beklenen 504 satir, uretilen: {satir_sayisi}")

        if args.dogrula_stub:
            stub_ile_karsilastir(spark, HEDEF_YOL, STUB_YOL)

        return 0
    except Exception as exc:
        print(f"[HATA] iot_esik uretimi basarisiz: {exc}", file=sys.stderr)
        return 1
    finally:
        if uzun_df is not None:
            uzun_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())