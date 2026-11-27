# -*- coding: utf-8 -*-
"""Üye C · Z10 Kirli Veri Kurallarının Tam Sayımı ve Çarpıklık Özeti"""
import sys
sys.path.insert(0, "/kit/araclar")
import duckdb
from ortak import S3_ADRES, S3_ANAHTAR, S3_GIZLI

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

con.execute("CREATE TABLE ham AS SELECT * FROM read_json_auto('s3://ham/iot/*/*.jsonl', format='newline_delimited')")
con.execute("CREATE TABLE sensorler AS SELECT * FROM read_csv_auto('s3://ham/boyut/iot/sensorler.csv')")

N = con.execute("SELECT count(*) FROM ham").fetchone()[0]

kurallar = [
    ("1. Tam tekrar satır (duplicate)", N - con.execute("SELECT count(*) FROM (SELECT DISTINCT * FROM ham)").fetchone()[0]),
    ("2. Eksik anahtar (sensor_id IS NULL)", con.execute("SELECT count(*) FROM ham WHERE sensor_id IS NULL").fetchone()[0]),
    ("3. Eksik bölge (bolge IS NULL)", con.execute("SELECT count(*) FROM ham WHERE bolge IS NULL").fetchone()[0]),
    ("4. Eksik ölçüm (sicaklik/nem/titresim NULL)", con.execute("SELECT count(*) FROM ham WHERE sicaklik IS NULL OR nem IS NULL OR titresim IS NULL").fetchone()[0]),
    ("5. Fiziksel sınır dışı sıcaklık (<-50 veya >100 °C)", con.execute("SELECT count(*) FROM ham WHERE sicaklik < -50 OR sicaklik > 100").fetchone()[0]),
    ("6. Fiziksel sınır dışı nem (<0 veya >100 %)", con.execute("SELECT count(*) FROM ham WHERE nem < 0 OR nem > 100").fetchone()[0]),
    ("7. Fiziksel sınır dışı titreşim (<0)", con.execute("SELECT count(*) FROM ham WHERE titresim < 0").fetchone()[0]),
    ("8. Bozuk/gelecek olay_zamani (TRY_CAST NULL veya >2026-11-24)", con.execute("SELECT count(*) FROM ham WHERE TRY_CAST(olay_zamani AS TIMESTAMP) IS NULL OR TRY_CAST(olay_zamani AS TIMESTAMP) > TIMESTAMP '2026-11-24 00:00:00'").fetchone()[0]),
    ("9. Boyut tablosunda olmayan sensor_id", con.execute("SELECT count(*) FROM ham h LEFT JOIN sensorler s ON h.sensor_id = s.sensor_id WHERE h.sensor_id IS NOT NULL AND s.sensor_id IS NULL").fetchone()[0]),
    ("10. Bölge uyuşmazlığı (olay.bolge != sensor.bolge)", con.execute("SELECT count(*) FROM ham h JOIN sensorler s ON h.sensor_id = s.sensor_id WHERE h.bolge IS NOT NULL AND h.bolge != s.bolge").fetchone()[0]),
]

toplam_kirli = con.execute("""
    SELECT count(*) FROM ham h
    LEFT JOIN sensorler s ON h.sensor_id = s.sensor_id
    WHERE h.sensor_id IS NULL OR h.bolge IS NULL
       OR h.sicaklik IS NULL OR h.nem IS NULL OR h.titresim IS NULL
       OR h.sicaklik < -50 OR h.sicaklik > 100
       OR h.nem < 0 OR h.nem > 100
       OR h.titresim < 0
       OR TRY_CAST(h.olay_zamani AS TIMESTAMP) IS NULL
       OR TRY_CAST(h.olay_zamani AS TIMESTAMP) > TIMESTAMP '2026-11-24 00:00:00'
       OR (h.sensor_id IS NOT NULL AND s.sensor_id IS NULL)
       OR (h.bolge IS NOT NULL AND s.bolge IS NOT NULL AND h.bolge != s.bolge)
""").fetchone()[0]

satirlar = ["\n=== 4. Z10 DETAYLI KİRLİ KAYIT DÖKÜMÜ ==="]
for ad, sayi in kurallar:
    satirlar.append(f"  {ad:<55} : {sayi:>10,} (%{sayi/N*100:.3f})")
satirlar.append(f"  {'Tekil kirli kayıt (en az 1 kural ihlali, tekrar hariç)':<55} : {toplam_kirli:>10,} (%{toplam_kirli/N*100:.3f})")

satirlar.append("\n=== 5. DAĞILIM VE ÇARPIKLIK ÖZETİ (Z3 ÖN İNCELEME) ===")
satirlar.append("Bölge bazlı satır dağılımı:")
for b, c in con.execute("SELECT COALESCE(bolge, 'NULL'), count(*) c FROM ham GROUP BY 1 ORDER BY c DESC").fetchall():
    satirlar.append(f"  {b:<15} : {c:>10,} (%{c/N*100:.2f})")

satirlar.append("\nEn yoğun 5 sensör:")
for sid, c in con.execute("SELECT COALESCE(sensor_id, 'NULL'), count(*) c FROM ham GROUP BY 1 ORDER BY c DESC LIMIT 5").fetchall():
    satirlar.append(f"  {sid:<15} : {c:>10,} (%{c/N*100:.2f})")

cikti = "\n".join(satirlar)
print(cikti)
with open("/kit/kanit/c_tani_olcek_kirli.txt", "a", encoding="utf-8") as f:
    f.write(cikti + "\n")
