# -*- coding: utf-8 -*-
"""A1 — Son 10 dakikada normal dışına çıkan sensör (B3 prototipi + gecikme ölçümü).

İki yol (K-09b'de karşılaştırılıyor):
  anlik   : Her ölçüm geldiği mikro-yığında eşik tablosuyla karşılaştırılır (durumsuz filtre, watermark yok).
            Uyarı süresi ≈ trigger aralığı + yığın süresi. Geç gelen ölçüm de işaretlenir (geç ama kaybolmaz).
  pencere : 10 dk'lık sabit pencerede sensör başına anomali sayısı + en büyük sapma; watermark ile kesinleşir (append).
            Satır ancak pencere bitip watermark geçince çıkar → kesin ama geç.
Kurallar: K-04 earliest · K-09a eşik (bölge × saat, medyan ± 5 × dss) · bölge mesajdan değil sensorler.csv'den alınır
(mesajlarda bölge eksik olabiliyor, B1/B3) · sensor_id'siz kayıt atılır. Tekrar ayıklama B5'te (K-12) eklenecek.

Çalıştırma:
  docker compose exec spark spark-submit is/akis/a1_anomali.py --mod ikisi --trigger 2
  Ölçüm kipi: --olcum kanit/b3/gecikme.csv  (her yığın için gecikme istatistiği CSV'ye eklenir)
"""
import argparse
import csv
import os
import threading
import time

from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--mod", default="oran", choices=["oran", "anlik", "pencere", "ikisi"],
                help="oran = K-09b kararı; anlik/pencere/ikisi = B3 karşılaştırma deneyleri")
ap.add_argument("--oran-esik", type=float, default=1.0, help="K-09b: pencerede sıçrama oranı (%%) eşiği")
ap.add_argument("--min-olcum", type=int, default=200, help="K-09b: oranın değerlendirilmesi için en az ölçüm (B3'te ölçülüyor)")
ap.add_argument("--ham-kayit", default=None, help="oran kipi: her yığında güncellenen bütün pencere satırları (analiz için) CSV")
ap.add_argument("--trigger", type=float, default=2.0, help="mikro-yığın aralığı, saniye (K-08)")
ap.add_argument("--esik", default="s3a://islenmis/_stub/iot_esik.parquet", help="K-09a eşik tablosu (A'nınki gelince değişecek)")
ap.add_argument("--k", type=float, default=5.0)
ap.add_argument("--pencere", default="10 minutes")
ap.add_argument("--watermark", default="2 minutes", help="GEÇİCİ: kesin değer B6'da (K-13)")
ap.add_argument("--olcum", default=None, help="gecikme ölçümlerinin ekleneceği CSV (repo köküne göre)")
ap.add_argument("--etiket", default="")
ap.add_argument("--sure", type=int, default=0, help="hazır olduktan sonra kaç saniye çalışsın (0 = sonsuz)")
ap.add_argument("--hazir-dosyasi", default=None, help="iş veri beklemeye başlayınca oluşturulur (deney betiği için)")
a = ap.parse_args()

spark = (SparkSession.builder.appName(f"a1-{a.mod}")
         .config("spark.sql.session.timeZone", "UTC")  # saat = UTC; eşik tablosuyla aynı (K-09a)
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

SEMA = "olay_zamani STRING, sensor_id STRING, bolge STRING, sicaklik DOUBLE, nem DOUBLE, titresim DOUBLE"
sensor = spark.read.option("header", True).csv("s3a://ham/boyut/iot/sensorler.csv").select("sensor_id", "bolge")
esik = spark.read.parquet(a.esik)

ham = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", "sensorler").option("startingOffsets", "earliest").load())   # K-04

olay = (ham.select(F.from_json(F.col("value").cast("string"), SEMA).alias("o"), F.col("timestamp").alias("kafka_zamani"))
        .select("o.olay_zamani", "o.sensor_id", "o.sicaklik", "o.nem", "o.titresim", "kafka_zamani")
        .where(F.col("sensor_id").isNotNull())
        .withColumn("olay_zamani", F.to_timestamp("olay_zamani"))
        .join(F.broadcast(sensor), "sensor_id")                       # bölge boyut tablosundan
        .withColumn("saat", F.hour("olay_zamani")))

uzun = (olay.select("*", F.expr("stack(3, 'sicaklik', sicaklik, 'nem', nem, 'titresim', titresim) AS (olcu, deger)"))
        .where(F.col("deger").isNotNull())
        .join(F.broadcast(esik), ["bolge", "saat", "olcu"])
        .withColumn("sapma", F.abs(F.col("deger") - F.col("medyan")) / F.col("dss"))
        .withColumn("anomali", F.col("sapma") > a.k))


def olcum_yaz(satir):
    if not a.olcum:
        return
    yol = os.path.join("/kit", a.olcum)
    yeni = not os.path.exists(yol)
    with open(yol, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(satir))
        if yeni:
            w.writeheader()
        w.writerow(satir)


def anlik_yigin(df, yigin_id):
    df = df.persist()
    t_islem_bitti = None
    st = df.agg(F.count("*").alias("n"),
                F.min("kafka_zamani").alias("k_min"), F.max("kafka_zamani").alias("k_max"),
                F.max(F.col("kafka_zamani").cast("double") - F.col("olay_zamani").cast("double")).alias("en_gec_olay_s")).first()
    if st["n"]:
        df.select("olay_zamani", "sensor_id", "bolge", "olcu", "deger", F.round("sapma", 1).alias("kat")).show(3, truncate=False)
    t_islem_bitti = time.time()
    if st["n"]:
        k_min, k_max = st["k_min"].timestamp(), st["k_max"].timestamp()
        olcum_yaz(dict(etiket=a.etiket, yol="anlik", trigger_s=a.trigger, yigin=yigin_id, uyari=st["n"],
                       gecikme_en_kucuk_s=round(t_islem_bitti - k_max, 3),   # yığına en son giren olay
                       gecikme_en_buyuk_s=round(t_islem_bitti - k_min, 3),   # yığına en erken giren olay
                       olay_aninda_en_buyuk_s=round(st["en_gec_olay_s"] + (t_islem_bitti - k_max), 3), zaman=round(t_islem_bitti, 3)))
    df.unpersist()


def pencere_yigin(df, yigin_id):
    df = df.persist()
    n = df.count()
    t_islem_bitti = time.time()
    if n:
        df.orderBy(F.desc("anomali_sayisi")).show(3, truncate=False)
        st = df.where("esik_asildi").agg(F.count("*").alias("m"), F.min("ilk_anomali_kafka").alias("ilk"),
                                         F.max("ilk_anomali_kafka").alias("son")).first()
        olcum_yaz(dict(etiket=a.etiket, yol="pencere", trigger_s=a.trigger, yigin=yigin_id, uyari=st["m"] or 0,
                       gecikme_en_kucuk_s=round(t_islem_bitti - st["son"].timestamp(), 3) if st["m"] else "",
                       gecikme_en_buyuk_s=round(t_islem_bitti - st["ilk"].timestamp(), 3) if st["m"] else "",
                       olay_aninda_en_buyuk_s="", zaman=round(t_islem_bitti, 3)))
    df.unpersist()


# ── K-09b: 10 dk pencerede sensörün sıçrama ORANI (ölçüm = bir mesaj; herhangi bir ölçü eşiği aşarsa sıçrama) ──
esik_genis = (esik.groupBy("bolge", "saat").pivot("olcu", ["sicaklik", "nem", "titresim"])
              .agg(F.first("medyan").alias("m"), F.first("dss").alias("d")))


def asti(olcu):
    return F.coalesce(F.abs(F.col(olcu) - F.col(f"{olcu}_m")) > a.k * F.col(f"{olcu}_d"), F.lit(False))


oran = (olay.join(F.broadcast(esik_genis), ["bolge", "saat"])
        # üç ölçüsü de boş mesaj karşılaştırılamaz → paydaya girmesin (oranı yapay düşürmesin); kirli kayıt (Z10)
        .where(F.col("sicaklik").isNotNull() | F.col("nem").isNotNull() | F.col("titresim").isNotNull())
        .withColumn("anomali", asti("sicaklik") | asti("nem") | asti("titresim"))
        .withWatermark("olay_zamani", a.watermark)
        .groupBy(F.window("olay_zamani", a.pencere), "sensor_id")
        .agg(F.count("*").alias("olcum_sayisi"), F.sum(F.col("anomali").cast("int")).alias("anomali_sayisi"),
             F.max("kafka_zamani").alias("son_kafka"))
        .select(F.col("window.start").alias("pencere_bas"), F.col("window.end").alias("pencere_bit"),
                F.col("sensor_id").alias("anahtar"),
                F.round(100 * F.col("anomali_sayisi") / F.col("olcum_sayisi"), 3).alias("deger"),          # oran, %
                ((100 * F.col("anomali_sayisi") / F.col("olcum_sayisi") > a.oran_esik)
                 & (F.col("olcum_sayisi") >= a.min_olcum)).alias("esik_asildi"),
                "olcum_sayisi", "anomali_sayisi", "son_kafka"))
isaretli_once = set()


def oran_yigin(df, yigin_id):
    satirlar = df.collect()          # güncellenen pencere satırları (≈ etkin sensör sayısı kadar, küçük)
    t_bitti = time.time()
    if not satirlar:
        return
    if a.ham_kayit:                  # analiz için ham kayıt: her yığında her güncellenen satır
        yol = os.path.join("/kit", a.ham_kayit)
        yeni = not os.path.exists(yol)
        with open(yol, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if yeni:
                w.writerow(["etiket", "yigin", "t_bitti", "pencere_bas", "anahtar", "olcum_sayisi", "anomali_sayisi", "son_kafka"])
            for r in satirlar:
                w.writerow([a.etiket, yigin_id, round(t_bitti, 3), r["pencere_bas"], r["anahtar"], r["olcum_sayisi"],
                            r["anomali_sayisi"], round(r["son_kafka"].timestamp(), 3)])
    yeni_isaret = [r for r in satirlar if r["esik_asildi"] and (r["pencere_bas"], r["anahtar"]) not in isaretli_once]
    for r in yeni_isaret:
        isaretli_once.add((r["pencere_bas"], r["anahtar"]))
        print(f"⚠ yığın {yigin_id}: {r['anahtar']} normal dışı · pencere {r['pencere_bas']:%H:%M} · oran %{r['deger']} "
              f"({r['anomali_sayisi']}/{r['olcum_sayisi']}) · Kafka'dan {t_bitti - r['son_kafka'].timestamp():.2f} s sonra")


sorgular = []
etiket = a.etiket or f"t{a.trigger}"
if a.mod == "oran":
    sorgular.append(oran.writeStream.outputMode("update").foreachBatch(oran_yigin)
                    .option("checkpointLocation", f"/tmp/chk-a1-oran-{etiket}-{int(time.time())}")
                    .trigger(processingTime=f"{a.trigger} seconds").start())
if a.mod in ("anlik", "ikisi"):
    sorgular.append(uzun.where("anomali").writeStream.foreachBatch(anlik_yigin)
                    .option("checkpointLocation", f"/tmp/chk-a1-anlik-{etiket}-{int(time.time())}")
                    .trigger(processingTime=f"{a.trigger} seconds").start())
if a.mod in ("pencere", "ikisi"):
    pencere = (uzun.withWatermark("olay_zamani", a.watermark)
               .groupBy(F.window("olay_zamani", a.pencere), "sensor_id")
               .agg(F.sum(F.col("anomali").cast("int")).alias("anomali_sayisi"),
                    F.max("sapma").alias("en_buyuk_sapma"),
                    F.min(F.when(F.col("anomali"), F.col("kafka_zamani"))).alias("ilk_anomali_kafka"))
               .select(F.col("window.start").alias("pencere_bas"), F.col("window.end").alias("pencere_bit"),
                       F.col("sensor_id").alias("anahtar"), F.col("en_buyuk_sapma").alias("deger"),
                       (F.col("anomali_sayisi") > 0).alias("esik_asildi"), "anomali_sayisi", "ilk_anomali_kafka"))
    sorgular.append(pencere.writeStream.outputMode("append").foreachBatch(pencere_yigin)
                    .option("checkpointLocation", f"/tmp/chk-a1-pencere-{etiket}-{int(time.time())}")
                    .trigger(processingTime=f"{a.trigger} seconds").start())

# Deney betiği için: bütün sorgular veri beklemeye geçince "hazır" dosyası
if a.hazir_dosyasi:
    def hazir_bekle():
        while not all(q.status["message"].startswith("Waiting for") for q in sorgular):
            time.sleep(0.5)
        open(os.path.join("/kit", a.hazir_dosyasi), "w").close()
        print("HAZIR — veri bekleniyor")
    threading.Thread(target=hazir_bekle, daemon=True).start()

if a.sure:
    while not (a.hazir_dosyasi and os.path.exists(os.path.join("/kit", a.hazir_dosyasi))):
        time.sleep(0.5)
    time.sleep(a.sure)
    for q in sorgular:
        q.stop()
else:
    spark.streams.awaitAnyTermination()
