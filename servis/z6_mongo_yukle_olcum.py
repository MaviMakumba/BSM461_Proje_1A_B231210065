# -*- coding: utf-8 -*-
"""Üye C · Z6 Servis Deposu (MongoDB) Yükleme, İndeksleme ve Gecikme (p50/p95/p99) Ölçümü"""
import sys, time, random, statistics
from datetime import datetime, timezone, timedelta
sys.path.insert(0, "/kit/araclar")
import duckdb
from pymongo import MongoClient, ASCENDING, DESCENDING
from ortak import S3_ADRES, S3_ANAHTAR, S3_GIZLI, MONGO_ADRES

t_bas = time.perf_counter()

# 1. DuckDB ile S3 temiz Parquet + boyut tablosundan özetleri hesapla
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

con.execute("CREATE VIEW temiz AS SELECT * FROM read_parquet('s3://islenmis/iot/temiz/*/*.parquet', hive_partitioning=true)")
con.execute("CREATE VIEW sensorler AS SELECT * FROM read_csv_auto('s3://ham/boyut/iot/sensorler.csv')")

sensor_satirlari = con.execute("""
    SELECT
        t.sensor_id,
        s.bolge,
        s.model,
        CAST(s.kurulum_yili AS INT) AS kurulum_yili,
        count(*) AS toplam_olcum,
        sum(CASE WHEN t.anomali_bayragi THEN 1 ELSE 0 END) AS anomali_sayisi,
        round(avg(t.sicaklik), 2) AS ort_sicaklik,
        round(max(t.sicaklik), 2) AS maks_sicaklik,
        round(avg(t.nem), 2) AS ort_nem,
        round(avg(t.titresim), 3) AS ort_titresim,
        round(max(t.titresim), 3) AS maks_titresim,
        max(t.olay_zamani) AS son_gorulme
    FROM temiz t
    JOIN sensorler s ON t.sensor_id = s.sensor_id
    GROUP BY 1, 2, 3, 4
    ORDER BY t.sensor_id
""").fetchdf().to_dict(orient="records")

bolge_saat_satirlari = con.execute("""
    SELECT
        bolge,
        EXTRACT(HOUR FROM olay_ts)::INT AS saat,
        count(*) AS olcum_sayisi,
        sum(CASE WHEN anomali_bayragi THEN 1 ELSE 0 END) AS anomali_sayisi,
        round(avg(sicaklik), 2) AS ort_sicaklik,
        round(avg(nem), 2) AS ort_nem,
        round(avg(titresim), 3) AS ort_titresim
    FROM temiz
    GROUP BY 1, 2
    ORDER BY 1, 2
""").fetchdf().to_dict(orient="records")

ornek_anomaliler = con.execute("""
    SELECT
        olay_zamani AS pencere_bas,
        olay_zamani AS pencere_bit,
        sensor_id AS anahtar,
        bolge,
        model,
        round(sicaklik, 2) AS sicaklik,
        round(titresim, 3) AS titresim,
        round(sicaklik, 2) AS deger,
        true AS esik_asildi
    FROM temiz
    WHERE anomali_bayragi = true
    ORDER BY olay_ts DESC
    LIMIT 2000
""").fetchdf().to_dict(orient="records")

# 2. MongoDB'ye yaz ve indeksleri oluştur
client = MongoClient(MONGO_ADRES)
db = client["bsm461"]

db.sensor_ozet.drop()
db.bolge_saat_ozet.drop()
db.akis_a1_anomali.drop()
db.akis_a2_susan.drop()

db.sensor_ozet.insert_many(sensor_satirlari)
db.bolge_saat_ozet.insert_many(bolge_saat_satirlari)
if ornek_anomaliler:
    db.akis_a1_anomali.insert_many(ornek_anomaliler)

# Susan sensör koleksiyonu için başlangıç örneği (B5'teki akis_iot_a2 geldiğinde burası güncellenecek)
susan_ornek = [
    {
        "pencere_bas": s["son_gorulme"],
        "pencere_bit": s["son_gorulme"],
        "anahtar": s["sensor_id"],
        "bolge": s["bolge"],
        "model": s["model"],
        "deger": 0,
        "esik_asildi": False,
        "son_gorulme": s["son_gorulme"],
        "durum": "AKTIF"
    }
    for s in sensor_satirlari[:10]
]
db.akis_a2_susan.insert_many(susan_ornek)

# İndeksler (Z6 sorgu performansı için)
db.sensor_ozet.create_index([("sensor_id", ASCENDING)], unique=True)
db.sensor_ozet.create_index([("bolge", ASCENDING), ("anomali_sayisi", DESCENDING)])
db.bolge_saat_ozet.create_index([("bolge", ASCENDING), ("saat", ASCENDING)], unique=True)
db.akis_a1_anomali.create_index([("esik_asildi", ASCENDING), ("pencere_bit", DESCENDING)])
db.akis_a1_anomali.create_index([("anahtar", ASCENDING), ("pencere_bit", DESCENDING)])
db.akis_a2_susan.create_index([("esik_asildi", ASCENDING), ("son_gorulme", ASCENDING)])

yukleme_s = time.perf_counter() - t_bas

# 3. Z6 Gecikme Testi: 1.000 gerçekçi sorgu (p50, p95, p99)
random.seed(461)
sensor_id_listesi = [r["sensor_id"] for r in sensor_satirlari]
bolge_listesi = list({r["bolge"] for r in sensor_satirlari})
gecikmeler_ms = []

for i in range(1000):
    tur = i % 3
    t0 = time.perf_counter()
    if tur == 0:
        # Tekil sensör kartı sorgusu
        sid = random.choice(sensor_id_listesi)
        _ = db.sensor_ozet.find_one({"sensor_id": sid}, {"_id": 0})
    elif tur == 1:
        # Bölge bazlı en çok anomali üreten ilk 10 sensör
        b = random.choice(bolge_listesi)
        _ = list(db.sensor_ozet.find({"bolge": b}, {"_id": 0}).sort("anomali_sayisi", DESCENDING).limit(10))
    else:
        # Son eşik aşan anomaliler listesi
        _ = list(db.akis_a1_anomali.find({"esik_asildi": True}, {"_id": 0}).sort("pencere_bit", DESCENDING).limit(20))
    gecikmeler_ms.append((time.perf_counter() - t0) * 1000.0)

gecikmeler_ms.sort()
p50 = gecikmeler_ms[int(0.50 * len(gecikmeler_ms))]
p95 = gecikmeler_ms[int(0.95 * len(gecikmeler_ms))]
p99 = gecikmeler_ms[int(0.99 * len(gecikmeler_ms))]
maks = gecikmeler_ms[-1]

satirlar = [
    "=== Z6 SERVİS DEPOSU (MONGODB 7.0) YÜKLEME VE GECİKME ÖLÇÜMÜ ===",
    f"Veritabanı / Koleksiyonlar : bsm461 -> sensor_ozet ({db.sensor_ozet.count_documents({})}), "
    f"bolge_saat_ozet ({db.bolge_saat_ozet.count_documents({})}), akis_a1_anomali ({db.akis_a1_anomali.count_documents({})})",
    f"S3 Parquet -> MongoDB yükleme + indeksleme süresi : {yukleme_s:.2f} s",
    "Oluşturulan indeksler : sensor_id (unique), (bolge, anomali_sayisi), (bolge, saat), (esik_asildi, pencere_bit)",
    "",
    "--- Z6 Sorgu Gecikmesi (N = 1,000 karışık okuma sorgusu) ---",
    f"p50 (ortanca) gecikme : {p50:.2f} ms",
    f"p95 gecikme           : {p95:.2f} ms",
    f"p99 gecikme           : {p99:.2f} ms   (Senaryo hedefi: <= 2000 ms -> {'BAŞARILI ✅' if p99 <= 2000 else 'AŞILDI ❌'})",
    f"En yüksek (max)       : {maks:.2f} ms",
]

cikti = "\n".join(satirlar)
print(cikti)
with open("/kit/kanit/c_z6_servis_gecikme.txt", "w", encoding="utf-8") as f:
    f.write(cikti + "\n")
