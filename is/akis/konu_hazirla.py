# -*- coding: utf-8 -*-
"""Kafka konusunu kararlarımıza göre (yeniden) kurar — K-05 (3 bölüm), yönerge Adım 11.1.

Çalıştırma:  docker compose exec araclar python is/akis/konu_hazirla.py
             docker compose exec araclar python is/akis/konu_hazirla.py --sil-checkpoint   (deney öncesi, K-04)

- Konu varsa SİLER ve yeniden oluşturur → içindeki bütün mesajlar gider (deneylerin temiz başlaması için; K-04 earliest).
- Bölüm: 3 (K-05) · replika: 1 (kitte tek Kafka düğümü; üretimde 3 olmalı — Z0 garanti tablosu).
- --sil-checkpoint: akış işlerinin S3'teki checkpoint klasörünü de siler (s3a://islenmis/_checkpoint/iot/).
"""
import argparse
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, "araclar")
from ortak import KAFKA, s3  # noqa: E402  (kitin ortak ayarları: Kafka adresi, S3 istemcisi)

ap = argparse.ArgumentParser()
ap.add_argument("--konu", default="sensorler")
ap.add_argument("--bolum", type=int, default=3, help="K-05")
ap.add_argument("--replika", type=int, default=1)
ap.add_argument("--sil-checkpoint", action="store_true")
a = ap.parse_args()

yonetici = AdminClient({"bootstrap.servers": KAFKA})

if a.konu in yonetici.list_topics(timeout=10).topics:
    yonetici.delete_topics([a.konu])[a.konu].result()
    for _ in range(30):  # silme eşzamansız; konu listeden düşene kadar bekle
        if a.konu not in yonetici.list_topics(timeout=10).topics:
            break
        time.sleep(1)
    print(f"✓ '{a.konu}' silindi")

yonetici.create_topics([NewTopic(a.konu, num_partitions=a.bolum, replication_factor=a.replika)])[a.konu].result()
for _ in range(30):  # oluşturma da eşzamansız; bölüm bilgisi gelene kadar bekle
    bilgi = yonetici.list_topics(a.konu, timeout=10).topics[a.konu]
    if len(bilgi.partitions) == a.bolum:
        break
    time.sleep(1)
print(f"✓ '{a.konu}' oluşturuldu · bölüm={len(bilgi.partitions)} · replika={len(bilgi.partitions[0].replicas)}")

if a.sil_checkpoint:
    istemci = s3()
    silinen = 0
    for sayfa in istemci.get_paginator("list_objects_v2").paginate(Bucket="islenmis", Prefix="_checkpoint/iot/"):
        anahtarlar = [{"Key": o["Key"]} for o in sayfa.get("Contents", [])]
        if anahtarlar:
            istemci.delete_objects(Bucket="islenmis", Delete={"Objects": anahtarlar})
            silinen += len(anahtarlar)
    print(f"✓ checkpoint temizlendi (s3://islenmis/_checkpoint/iot/, {silinen} nesne)")
