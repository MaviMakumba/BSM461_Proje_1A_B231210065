# -*- coding: utf-8 -*-
"""Üye C · IoT Şablonu: Adım 3 (Veri Tanıma), Z1 (Ölçek) ve Z10 (Kirli Veri Sayımı)"""
import sys, time
sys.path.insert(0, "/kit/araclar")
import duckdb
from ortak import s3, S3_ADRES, S3_ANAHTAR, S3_GIZLI

istemci = s3()
dosya_sayisi = toplam_bayt = 0
for sayfa in istemci.get_paginator("list_objects_v2").paginate(Bucket="ham", Prefix="iot/"):
    for o in sayfa.get("Contents", []):
        if o["Key"].endswith(".jsonl"):
            dosya_sayisi += 1
            toplam_bayt += o["Size"]

con = duckdb.connect()
con.execute(f"""
    LOAD httpfs;
    SET s3_endpoint='{S3_ADRES.split('//')[1]}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_access_key_id='{S3_ANAHTAR}';
    SET s3_secret_access_key='{S3_GIZLI}';
    SET s3_region='us-east-1';
""")

t0 = time.perf_counter()
con.execute("CREATE TABLE ham AS SELECT * FROM read_json_auto('s3://ham/iot/*/*.jsonl', format='newline_delimited')")
con.execute("CREATE TABLE sensorler AS SELECT * FROM read_csv_auto('s3://ham/boyut/iot/sensorler.csv')")
okuma_s = time.perf_counter() - t0

N = con.execute("SELECT count(*) FROM ham").fetchone()[0]
ort_bayt = toplam_bayt / N

satirlar = []
satirlar.append("=== 1. HAM VERİ VE SÖZLEŞME ÖZETİ (ADIM 3) ===")
satirlar.append(f"Dosya sayısı : {dosya_sayisi} · Toplam boyut: {toplam_bayt / (1024**2):.2f} MB ({toplam_bayt:,} bayt)")
satirlar.append(f"Toplam kayıt : {N:,} · Olay başı JSON: {ort_bayt:.2f} bayt · DuckDB okuma: {okuma_s:.2f} s\n")

satirlar.append("Ham Olay Sütunları (s3://ham/iot/*/*.jsonl):")
alan_bilgi = con.execute("DESCRIBE ham").fetchall()
alanlar = [r[0] for r in alan_bilgi]
satirlar.append(f"  {'Alan':<18} {'Tip':<12} {'Dolu':>12} {'Boş/Null':>12} {'Farklı Değer':>14}")
for a, tip, *_ in alan_bilgi:
    dolu, farkli = con.execute(f"SELECT count({a}), count(DISTINCT {a}) FROM ham").fetchone()
    satirlar.append(f"  {a:<18} {tip:<12} {dolu:>12,} {N - dolu:>12,} {farkli:>14,}")

satirlar.append("\nBoyut Tablosu Sütunları (sensorler.csv):")
for r in con.execute("DESCRIBE sensorler").fetchall():
    satirlar.append(f"  {r[0]} ({r[1]})")

satirlar.append("\nÖrnek 5 Satır (Ham Veri):")
for r in con.execute("SELECT * FROM ham USING SAMPLE 5 ROWS (reservoir, 461)").fetchall():
    satirlar.append("  " + str(dict(zip(alanlar, r))))

satirlar.append("\n=== 2. Z1 ÖLÇEK VE EŞİK HESABI (SENARYO KARTI) ===")
ORT_HIZ, TEPE_CARPAN, SAKLAMA_GUN = 2000, 2, 30
gunluk_olay = ORT_HIZ * 86400
gunluk_gb = (gunluk_olay * ort_bayt) / (1024**3)
toplam_gb = gunluk_gb * SAKLAMA_GUN
tepe_hiz = ORT_HIZ * TEPE_CARPAN
tepe_mbps = (tepe_hiz * ort_bayt * 8) / 1e6

satirlar.append(f"Varsayımlar      : {ORT_HIZ:,} ölçüm/s ort. · Tepe x{TEPE_CARPAN} · {SAKLAMA_GUN} gün saklama · 1 kopya ham")
satirlar.append(f"Günlük olay      : {gunluk_olay:,} olay/gün")
satirlar.append(f"Günlük ham hacim : {gunluk_gb:.2f} GB/gün")
satirlar.append(f"30 günlük ham    : {toplam_gb:.2f} GB (3 kopya S3/HDFS ile {toplam_gb*3:.2f} GB)")
satirlar.append(f"Tepe yük hızı    : {tepe_hiz:,} olay/s (~{tepe_mbps:.2f} Mbps)")

satirlar.append("\n=== 3. Z10 KİRLİ VERİ SAYIMI ===")
tam_tekrar = N - con.execute("SELECT count(*) FROM (SELECT DISTINCT * FROM ham)").fetchone()[0]
satirlar.append(f"Tamamen aynı (tekrar) satır : {tam_tekrar:,} (%{tam_tekrar/N*100:.3f})")

cikti = "\n".join(satirlar)
print(cikti)
with open("/kit/kanit/c_tani_olcek_kirli.txt", "w", encoding="utf-8") as f:
    f.write(cikti)
