# -*- coding: utf-8 -*-
"""B3 — Arızalı sensörlerin (toplu veride oran > %1) sıçramaları zamana nasıl dağılıyor? Sürekli mi, atak (epizot) hâlinde mi?
Çalıştırma: docker compose exec araclar python is/olcum/b3_ariza_zamani.py
"""
import duckdb
con = duckdb.connect(); con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='s3:8333'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_access_key_id='bsm461';"
            "SET s3_secret_access_key='bsm461-gizli'; SET s3_region='us-east-1';")
con.execute("""CREATE TABLE a AS
WITH t AS (SELECT DISTINCT h.sensor_id, s.bolge, s.model, TRY_CAST(h.olay_zamani AS TIMESTAMP) z, h.sicaklik, h.nem, h.titresim
 FROM read_json('s3://ham/iot/tarih=*/*.jsonl', format='newline_delimited',
   columns={'olay_zamani':'VARCHAR','sensor_id':'VARCHAR','sicaklik':'DOUBLE','nem':'DOUBLE','titresim':'DOUBLE'}) h
 JOIN read_csv('s3://ham/boyut/iot/sensorler.csv', header=true) s USING (sensor_id)),
e AS (SELECT * FROM 's3://islenmis/_stub/iot_esik.parquet')
SELECT t.sensor_id, t.model, t.z,
 ((abs(t.sicaklik-es.medyan)>5*es.dss) OR (abs(t.nem-en.medyan)>5*en.dss) OR (abs(t.titresim-et.medyan)>5*et.dss)) anomali
FROM t JOIN e es ON es.bolge=t.bolge AND es.saat=hour(t.z) AND es.olcu='sicaklik'
JOIN e en ON en.bolge=t.bolge AND en.saat=hour(t.z) AND en.olcu='nem'
JOIN e et ON et.bolge=t.bolge AND et.saat=hour(t.z) AND et.olcu='titresim'""")
def yaz(b, q):
    print(f"\n## {b}"); r = con.sql(q); print(" | ".join(r.columns))
    for x in r.fetchall(): print(" | ".join(str(v) for v in x))
A = "sensor_id IN ('sensor-050','sensor-051','sensor-052','sensor-053','sensor-054','sensor-055','sensor-056','sensor-057','sensor-058','sensor-059')"
yaz("Arızalı 10 sensör: güne göre anomali oranı (%) ↔ diğer 190", f"""
SELECT CAST(z AS DATE) gun, round(100*avg(anomali::INT) FILTER (WHERE {A}),2) arizali10,
  round(100*avg(anomali::INT) FILTER (WHERE NOT {A}),3) diger190 FROM a GROUP BY 1 ORDER BY 1""")
yaz("Arızalı 10 sensör: günün saatine göre anomali oranı (%)", f"""
SELECT hour(z) saat, round(100*avg(anomali::INT) FILTER (WHERE {A}),2) arizali10,
  round(100*avg(anomali::INT) FILTER (WHERE NOT {A}),3) diger190 FROM a GROUP BY 1 ORDER BY 1""")
yaz("Atak saatleri: arızalı sensörde oranı > %1 olan (sensör, gün, saat) dilimleri — kaç tane, ne kadar yoğun", f"""
WITH h AS (SELECT sensor_id, CAST(z AS DATE) gun, hour(z) saat, count(*) n, 100*avg(anomali::INT) oran FROM a WHERE {A} GROUP BY ALL)
SELECT count(*) FILTER (WHERE oran > 1) atakli_saat, count(*) toplam_saat,
  round(100.0*count(*) FILTER (WHERE oran > 1)/count(*),1) atakli_yuzde,
  round(median(oran) FILTER (WHERE oran > 1),1) atakta_oran_medyan, round(max(oran),1) atakta_oran_maks,
  round(median(oran) FILTER (WHERE oran <= 1),2) atak_disi_oran_medyan, round(avg(n),1) saat_basina_olcum FROM h""")
yaz("Sensör başına atak saatleri (gün:saat listesi)", f"""
WITH h AS (SELECT sensor_id, CAST(z AS DATE) gun, hour(z) saat, 100*avg(anomali::INT) oran FROM a WHERE {A} GROUP BY ALL)
SELECT sensor_id, count(*) atak_saati, string_agg(strftime(gun,'%d') || ':' || saat, ' ' ORDER BY gun, saat) dilimler
FROM h WHERE oran > 1 GROUP BY 1 ORDER BY 1""")
yaz("Ardışık atak saatleri birleşince atak süresi (saat)", f"""
WITH h AS (SELECT sensor_id, epoch(date_trunc('hour', z))/3600 s FROM a WHERE {A} GROUP BY sensor_id, date_trunc('hour', z)
           HAVING 100*avg(anomali::INT) > 1),
g AS (SELECT sensor_id, s, s - row_number() OVER (PARTITION BY sensor_id ORDER BY s) grp FROM h)
SELECT sure_saat, count(*) atak_sayisi FROM (SELECT sensor_id, grp, count(*) sure_saat FROM g GROUP BY ALL) GROUP BY 1 ORDER BY 1""")
yaz("Ortak atak: her gün arızalı 10 sensörün EN ÇOK anomali verdiği saat · atak süresi ve atak içi oran", f"""
WITH h AS (SELECT CAST(z AS DATE) gun, date_trunc('hour', z) saat, count(*) FILTER (WHERE anomali) an FROM a WHERE {A} GROUP BY ALL),
tepe AS (SELECT gun, arg_max(saat, an) saat FROM h GROUP BY gun),
p AS (SELECT a.*, tepe.gun FROM a JOIN tepe ON a.z >= tepe.saat - INTERVAL 1 HOUR AND a.z < tepe.saat + INTERVAL 2 HOUR WHERE {A}),
s AS (SELECT gun, min(z) FILTER (WHERE anomali) ilk, max(z) FILTER (WHERE anomali) son FROM p GROUP BY gun)
SELECT s.gun, strftime(ilk,'%H:%M') atak_bas, strftime(son,'%H:%M') atak_bit, round((epoch(son)-epoch(ilk))/60,1) sure_dk,
  count(DISTINCT p.sensor_id) FILTER (WHERE p.anomali) sensor, count(*) FILTER (WHERE p.anomali) anomali,
  count(*) olcum, round(100.0*count(*) FILTER (WHERE p.anomali)/count(*),1) atak_ici_oran
FROM s JOIN p ON p.gun=s.gun AND p.z BETWEEN s.ilk AND s.son GROUP BY ALL ORDER BY s.gun""")
yaz("Aynı saatlerde diğer 15 TX-200 ve diğer modeller de etkileniyor mu? (atak dilimi içinde oran %)", f"""
WITH h AS (SELECT CAST(z AS DATE) gun, date_trunc('hour', z) saat, count(*) FILTER (WHERE anomali) an FROM a WHERE {A} GROUP BY ALL),
tepe AS (SELECT gun, arg_max(saat, an) saat FROM h GROUP BY gun)
SELECT CASE WHEN {A} THEN 'arizali10' WHEN model='TX-200' THEN 'diger TX-200' ELSE 'diger modeller' END grup,
  round(100*avg(anomali::INT),2) atak_saatinde_oran
FROM a JOIN tepe ON date_trunc('hour', a.z) = tepe.saat GROUP BY 1 ORDER BY 1""")
