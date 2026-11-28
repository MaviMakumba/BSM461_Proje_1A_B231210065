# -*- coding: utf-8 -*-
"""B3 / K-09d — Enjeksiyon doğru mu yapıldı? Atak sırasında saldırılan sensörlerin ölçümlerinin kaçta kaçı normal dışıydı?

Çalıştırma: docker compose exec spark spark-submit is/olcum/b3_enjeksiyon_pay.py <etiket>
Kafka'daki 'sensorler' konusunu TOPLU okur (deney bitince, konu henüz silinmeden). Hedef: toplu veride ölçülen atak payı %50,5.
Ayrıca normal üreticinin sensör başına hızı (enjeksiyon hızını 10/s seçmemizin dayanağı).
"""
import csv
import sys

from pyspark.sql import SparkSession, functions as F

et = sys.argv[1]
spark = SparkSession.builder.appName("enjeksiyon-pay").config("spark.sql.session.timeZone", "UTC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
SEMA = "olay_zamani STRING, sensor_id STRING, bolge STRING, sicaklik DOUBLE, nem DOUBLE, titresim DOUBLE"
k = (spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "sensorler")
     .option("startingOffsets", "earliest").load()
     .select(F.from_json(F.col("value").cast("string"), SEMA).alias("o"), F.col("timestamp").cast("double").alias("kz"))
     .select("o.*", "kz").where(F.col("sensor_id").isNotNull())
     .withColumn("saat", F.hour(F.to_timestamp("olay_zamani"))))
sensor = spark.read.option("header", True).csv("s3a://ham/boyut/iot/sensorler.csv").select("sensor_id", F.col("bolge").alias("b"))
esik = (spark.read.parquet("s3a://islenmis/_stub/iot_esik.parquet").groupBy("bolge", "saat")
        .pivot("olcu", ["sicaklik", "nem", "titresim"]).agg(F.first("medyan").alias("m"), F.first("dss").alias("d")))


def asti(o):
    return F.coalesce(F.abs(F.col(o) - F.col(f"{o}_m")) > 5 * F.col(f"{o}_d"), F.lit(False))


k = (k.drop("bolge").join(sensor, "sensor_id").withColumnRenamed("b", "bolge").join(esik, ["bolge", "saat"])
     .where(F.col("sicaklik").isNotNull() | F.col("nem").isNotNull() | F.col("titresim").isNotNull())
     .withColumn("anomali", asti("sicaklik") | asti("nem") | asti("titresim")).cache())
toplam_s = k.agg(F.min("kz"), F.max("kz")).first()
sure = toplam_s[1] - toplam_s[0]
print(f"## Normal üretici: {k.count():,} ölçüm, {sure:.0f} s → sensör başına ortalama "
      f"{k.count() / sure / 200:.2f} ölçüm/s (200 sensör; enjeksiyon dahil, fark ihmal edilebilir)")
print("\n## Atak aralığında saldırılan sensörler (Kafka zamanına göre)\natak | konum_s | olcum | anomali | anomali_payi_%")
for at in csv.DictReader(open("/kit/kanit/b3/enjeksiyon-ataklar.csv", encoding="utf-8")):
    if at["etiket"] != et:
        continue
    b, s = (int(x) for x in at["sensorler"].split("-"))
    r = (k.where(F.col("sensor_id").isin([f"sensor-{i:03d}" for i in range(b, s + 1)])
         & F.col("kz").between(float(at["atak_bas"]), float(at["atak_bit"])))
         .agg(F.count("*").alias("n"), F.sum(F.col("anomali").cast("int")).alias("a")).first())
    print(f"{at['atak']} | {at['konum_s']} | {r['n']} | {r['a']} | {100 * r['a'] / r['n']:.1f}")
r = k.where(F.col("sensor_id").isin([f"sensor-{i:03d}" for i in range(50, 60)]) == False).agg(  # noqa: E712
    F.count("*").alias("n"), F.sum(F.col("anomali").cast("int")).alias("a")).first()
print(f"\n## Saldırılmayan 190 sensör, tüm deney: {r['n']:,} ölçüm, anomali payı %{100 * r['a'] / r['n']:.3f}")
spark.stop()
