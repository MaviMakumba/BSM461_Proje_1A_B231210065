# -*- coding: utf-8 -*-
"""B2 — Kafka bölüm sayısı ↔ Spark mikro-yığın süresi ölçümü (K-05).

Soru: Tepe yükte (4.000 olay/s) 5 saniyelik bir mikro-yığın (~20.000 olay) kaç ms'de işleniyor ve bu süre
Kafka bölüm sayısına göre nasıl değişiyor? 5 s uyarı süresi için yığın süresi < 5.000 ms olmalı.

Kullanım (spark konteynerinde):
  1) kopyala: aynı veriyi farklı bölüm sayılı konulara yazar (anahtar korunur → bölüm = hash(anahtar) % N)
     spark-submit is/olcum/b2_bolum_kapasite.py kopyala --kaynak b2-hiz-4000 --hedefler b2-p1,b2-p3,b2-p6
     (hedef konular önceden kafka-topics.sh --create --partitions N ile oluşturulur)
  2) olc: konuyu baştan, yığın başına --yigin olayla akış olarak işler; her yığının süresini yazar
     spark-submit is/olcum/b2_bolum_kapasite.py olc --konu b2-p3 --yigin 20000 --tekrar 3
İş yükü A1'e benzer: JSON ayrıştır → sensor_id boş olanları at → watermark 2 dk → 1 dk pencere × sensor_id
toplulaştırma (ort/maks) → noop çıkış (yalnızca işleme süresi ölçülsün, çıkış maliyeti B5'te ayrıca).
"""
import argparse
import shutil
import statistics
import time
import uuid

from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("kip", choices=["kopyala", "olc"])
ap.add_argument("--kaynak")
ap.add_argument("--hedefler")
ap.add_argument("--konu")
ap.add_argument("--yigin", type=int, default=20000, help="maxOffsetsPerTrigger (tepe 4.000/s × 5 s = 20.000)")
ap.add_argument("--tekrar", type=int, default=3)
a = ap.parse_args()

BOOT = "kafka:29092"
SEMA = "olay_zamani STRING, sensor_id STRING, bolge STRING, sicaklik DOUBLE, nem DOUBLE, titresim DOUBLE"
spark = SparkSession.builder.appName(f"b2-{a.kip}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

if a.kip == "kopyala":
    df = (spark.read.format("kafka").option("kafka.bootstrap.servers", BOOT)
          .option("subscribe", a.kaynak).option("startingOffsets", "earliest").load()
          .select("key", "value").cache())
    print(f"kaynak {a.kaynak}: {df.count()} mesaj")
    for h in a.hedefler.split(","):
        df.write.format("kafka").option("kafka.bootstrap.servers", BOOT).option("topic", h).save()
        print(f"→ {h} yazıldı")
    raise SystemExit

sonuclar = []
for t in range(1, a.tekrar + 1):
    chk = f"/tmp/b2-chk-{uuid.uuid4().hex}"
    ham = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
           .option("subscribe", a.konu).option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", a.yigin).load())
    olay = (ham.select(F.from_json(F.col("value").cast("string"), SEMA).alias("o")).select("o.*")
            .where(F.col("sensor_id").isNotNull())
            .withColumn("olay_zamani", F.to_timestamp("olay_zamani")))
    pencere = (olay.withWatermark("olay_zamani", "2 minutes")
               .groupBy(F.window("olay_zamani", "1 minute"), "sensor_id")
               .agg(F.avg("sicaklik"), F.max("sicaklik"), F.avg("nem"), F.max("titresim"), F.count("*")))
    q = (pencere.writeStream.outputMode("update").format("noop")
         .option("checkpointLocation", chk).trigger(availableNow=True).start())
    q.awaitTermination()
    ilerleme = [p for p in q.recentProgress if p["numInputRows"] > 0]
    shutil.rmtree(chk, ignore_errors=True)
    # ilk yığın JVM ısınması içerir → ayrı raporlanır, ortalamaya katılmaz
    sureler = [p["durationMs"]["triggerExecution"] for p in ilerleme]
    satir = [p["numInputRows"] for p in ilerleme]
    sicak = sureler[1:] or sureler
    print(f"[{a.konu} · tekrar {t}] yığın sayısı={len(sureler)} · olay/yığın={satir} · süre ms={sureler}")
    sonuclar.append((sureler[0], statistics.mean(sicak), max(sicak),
                     sum(satir[1:]) / (sum(sicak) / 1000) if len(sureler) > 1 else float("nan")))

print("\nÖZET", a.konu, f"yığın={a.yigin}")
for i, (ilk, ort, mak, hiz) in enumerate(sonuclar, 1):
    print(f"  tekrar {i}: ilk yığın {ilk} ms · sonraki yığınlar ort {ort:.0f} ms, maks {mak} ms · işleme hızı {hiz:.0f} olay/s")
print(f"  ORTALAMA ({len(sonuclar)} tekrar): yığın süresi {statistics.mean(r[1] for r in sonuclar):.0f} ms · "
      f"işleme hızı {statistics.mean(r[3] for r in sonuclar):.0f} olay/s")
