# -*- coding: utf-8 -*-
"""GEÇİCİ eşik tablosu (stub) — K-09a. A'nın gerçek tablosu (`s3a://tablolar/iot_esik`) gelene kadar akış işi bunu okur.

Çalıştırma: docker compose exec araclar python is/akis/esik_stub.py
Çıktı: s3://islenmis/_stub/iot_esik.parquet → (bolge, saat, olcu, medyan, dss)  · 7 bölge × 24 saat × 3 ölçü
Kural (K-09a): |değer − medyan| > 5 × dss ise anomali. dss = 1,4826 × MAD (dirençli standart sapma). Saat = olay_zamani'nın saati (UTC).
Girdi: toplu ham veri (tekrarlar ayıklanır, bölge sensorler.csv'den alınır).
"""
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='s3:8333'; SET s3_use_ssl=false; SET s3_url_style='path';"
            "SET s3_access_key_id='bsm461'; SET s3_secret_access_key='bsm461-gizli'; SET s3_region='us-east-1';")
con.execute("""
CREATE TABLE t AS SELECT DISTINCT s.bolge, hour(TRY_CAST(h.olay_zamani AS TIMESTAMP)) saat, h.sicaklik, h.nem, h.titresim
FROM read_json('s3://ham/iot/tarih=*/*.jsonl', format='newline_delimited',
     columns={'olay_zamani':'VARCHAR','sensor_id':'VARCHAR','sicaklik':'DOUBLE','nem':'DOUBLE','titresim':'DOUBLE'}) h
JOIN read_csv('s3://ham/boyut/iot/sensorler.csv', header=true) s USING (sensor_id)""")
con.execute("""
COPY (
  SELECT bolge, saat, 'sicaklik' olcu, median(sicaklik) medyan, 1.4826*mad(sicaklik) dss FROM t GROUP BY ALL
  UNION ALL SELECT bolge, saat, 'nem', median(nem), 1.4826*mad(nem) FROM t GROUP BY ALL
  UNION ALL SELECT bolge, saat, 'titresim', median(titresim), 1.4826*mad(titresim) FROM t GROUP BY ALL
  ORDER BY bolge, saat, olcu
) TO 's3://islenmis/_stub/iot_esik.parquet' (FORMAT parquet)""")
n, dmin, dmax = con.execute("""SELECT count(*), min(dss), max(dss) FROM 's3://islenmis/_stub/iot_esik.parquet'""").fetchone()
print(f"✓ s3://islenmis/_stub/iot_esik.parquet · {n} satır · dss aralığı {dmin:.3f}–{dmax:.3f}")
print(con.sql("SELECT olcu, round(min(dss),3) dss_min, round(max(dss),3) dss_max, count(*) satir "
              "FROM 's3://islenmis/_stub/iot_esik.parquet' GROUP BY olcu"))
