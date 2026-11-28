# -*- coding: utf-8 -*-
"""B3 / K-09d — Arıza enjeksiyonu: normal akış sürerken, toplu veride ÖLÇÜLEN atak davranışını Kafka'ya basar.

Çalıştırma (deney betiği çağırır): docker compose exec araclar python is/olcum/b3_ariza_enjeksiyonu.py --ilk-pencere <epoch> ...
Ne yapar:
  - Arızalı grup (varsayılan sensor-050…059) için, 10 dk'lık pencerenin belirli bir saniyesinde (--konumlar) atak başlatır.
    Her konum ayrı bir pencerede: 1. konum ilk pencerede, 2. konum bir sonrakinde …
  - Atak boyunca her sensör için saniyede --hiz-sensor ANORMAL ölçüm basar. Normal üretici (--hiz 2000) sensör başına ~10/s
    gönderdiğinden, varsayılan 10/s ile sensörün ölçümlerinin ~yarısı anormal olur → toplu veride ölçülen atak payı (%50,5).
  - Değer = (sensörün bölgesi × şu anki UTC saat) medyanı + toplu veride atak saatinde ölçülmüş bir sapma satırı
    (kanit/b3/atak-sapmalari.csv; sıcaklık/nem/titreşim sapmaları birlikte, aynı ölçümden).
  - Mesaj biçimi üreticiyle aynı (olay_zamani ISO 'Z', anahtar = sensor_id, K-06), acks=all + idempotent (K-07).
  - Her atağın başlangıç anı (ilk mesajın gönderildiği duvar saati) --kayit CSV'sine yazılır → tespit süresi = ⚠ anı − bu an.
"""
import argparse
import csv
import json
import random
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/kit/araclar")
import duckdb
from confluent_kafka import Producer
from ortak import KAFKA

ap = argparse.ArgumentParser()
ap.add_argument("--ilk-pencere", type=int, required=True, help="ilk atağın yapılacağı pencerenin başlangıcı (epoch s, 600'ün katı)")
ap.add_argument("--konumlar", default="5,300,570,595", help="pencere içinde atağın başladığı saniyeler (her biri ayrı pencerede)")
ap.add_argument("--atak-sure", type=float, default=30, help="atak kaç saniye sürsün")
ap.add_argument("--hiz-sensor", type=float, default=10, help="sensör başına saniyede anormal ölçüm")
ap.add_argument("--sensorler", default="50-59")
ap.add_argument("--pencere-s", type=int, default=600)
ap.add_argument("--kayit", default="kanit/b3/enjeksiyon-ataklar.csv")
ap.add_argument("--etiket", default="")
ap.add_argument("--tohum", type=int, default=461)
a = ap.parse_args()

r = random.Random(a.tohum)
bas, son = (int(x) for x in a.sensorler.split("-"))
SENSORLER = [f"sensor-{i:03d}" for i in range(bas, son + 1)]

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='s3:8333'; SET s3_use_ssl=false; SET s3_url_style='path';"
            "SET s3_access_key_id='bsm461'; SET s3_secret_access_key='bsm461-gizli'; SET s3_region='us-east-1';")
BOLGE = dict(con.sql("SELECT sensor_id, bolge FROM read_csv('s3://ham/boyut/iot/sensorler.csv', header=true)").fetchall())
MEDYAN = {(b, s, o): m for b, s, o, m in con.sql("SELECT bolge, saat, olcu, medyan FROM 's3://islenmis/_stub/iot_esik.parquet'").fetchall()}
SAPMA = con.sql("SELECT d_sicaklik, d_nem, d_titresim FROM read_csv('/kit/kanit/b3/atak-sapmalari.csv', header=true)").fetchall()
print(f"→ {len(SENSORLER)} sensör · {len(SAPMA)} ölçülmüş atak sapması · konumlar {a.konumlar} · {a.hiz_sensor}/s/sensör · {a.atak_sure} s")

p = Producer({"bootstrap.servers": KAFKA, "acks": "all", "enable.idempotence": True, "linger.ms": 5})
p.list_topics(timeout=10)                                  # bağlantı + idempotent kimlik atak öncesi hazır olsun


def mesaj(sid, an):
    ds, dn, dt = r.choice(SAPMA)
    b, saat = BOLGE[sid], an.hour

    def deger(olcu, d, basamak):                           # toplu veride o alan boşsa (kirli kayıt) burada da boş
        return None if d is None else round(max(0.0, MEDYAN[(b, saat, olcu)] + d) if olcu == "titresim" else MEDYAN[(b, saat, olcu)] + d, basamak)
    return {"olay_zamani": an.strftime("%Y-%m-%dT%H:%M:%S.") + f"{an.microsecond // 1000:03d}Z", "sensor_id": sid, "bolge": b,
            "sicaklik": deger("sicaklik", ds, 2), "nem": deger("nem", dn, 1), "titresim": deger("titresim", dt, 3)}


yeni = True
try:
    yeni = open("/kit/" + a.kayit, encoding="utf-8").readline() == ""
except FileNotFoundError:
    pass
kayit = open("/kit/" + a.kayit, "a", newline="", encoding="utf-8")
w = csv.writer(kayit)
if yeni:
    w.writerow(["etiket", "atak", "konum_s", "pencere_bas", "ilk_gonderim", "atak_bas", "atak_bit", "sensorler", "gonderilen"])

aralik = 1.0 / a.hiz_sensor
for i, konum in enumerate(int(x) for x in a.konumlar.split(",")):
    pencere = a.ilk_pencere + i * a.pencere_s
    hedef = pencere + konum
    bekle = hedef - time.time()
    if bekle < 0:
        print(f"✗ atak {i}: hedef an geçmiş ({-bekle:.1f} s), atlanıyor"); continue
    print(f"… atak {i}: konum {konum} s · pencere {datetime.fromtimestamp(pencere, timezone.utc):%H:%M} · {bekle:.0f} s bekleniyor", flush=True)
    time.sleep(bekle)
    atak_bas, gonderilen, tik, ilk_gonderim = None, 0, hedef, time.time()
    while time.time() < hedef + a.atak_sure:
        an = datetime.now(timezone.utc)
        for sid in SENSORLER:                              # her tikte her sensöre bir ölçüm → sensör başına hiz-sensor/s
            p.produce("sensorler", key=sid.encode(), value=json.dumps(mesaj(sid, an), ensure_ascii=False).encode("utf-8"))
            gonderilen += 1
        p.poll(0)
        if atak_bas is None:
            p.flush(5)                                     # ilk yığın Kafka'ya ulaştı (onaylandı) → atağın başlangıç anı
            atak_bas = time.time()
        tik += aralik
        time.sleep(max(0.0, tik - time.time()))
    p.flush(10)
    atak_bit = time.time()
    w.writerow([a.etiket, i, konum, datetime.fromtimestamp(pencere, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                round(ilk_gonderim, 3), round(atak_bas, 3), round(atak_bit, 3), a.sensorler, gonderilen])
    kayit.flush()
    print(f"✓ atak {i}: {gonderilen} anormal ölçüm · {atak_bit - atak_bas:.1f} s", flush=True)
kayit.close()
