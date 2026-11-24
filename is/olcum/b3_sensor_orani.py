# -*- coding: utf-8 -*-
"""B3 — Sensör başına anomali ORANI dağılımı (K-09a kuralıyla: bölge×saat, medyan ± 5·dss). Toplu 3M veri.
Soru: Tek tek sıçramalar her sensörde var mı? "Normal dışı SENSÖR"ü oranla ayırabilir miyiz?
Çalıştırma: docker compose exec araclar python is/olcum/b3_sensor_orani.py
"""
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='s3:8333'; SET s3_use_ssl=false; SET s3_url_style='path';"
            "SET s3_access_key_id='bsm461'; SET s3_secret_access_key='bsm461-gizli'; SET s3_region='us-east-1';")
con.execute("""CREATE TABLE t AS SELECT DISTINCT h.sensor_id, s.bolge, s.model, s.kurulum_yili,
  TRY_CAST(h.olay_zamani AS TIMESTAMP) zaman, h.sicaklik, h.nem, h.titresim
FROM read_json('s3://ham/iot/tarih=*/*.jsonl', format='newline_delimited',
  columns={'olay_zamani':'VARCHAR','sensor_id':'VARCHAR','sicaklik':'DOUBLE','nem':'DOUBLE','titresim':'DOUBLE'}) h
JOIN read_csv('s3://ham/boyut/iot/sensorler.csv', header=true) s USING (sensor_id)""")
con.execute("CREATE TABLE e AS SELECT * FROM 's3://islenmis/_stub/iot_esik.parquet'")
con.execute("""CREATE TABLE a AS
SELECT t.sensor_id, t.model, t.kurulum_yili, t.zaman,
  (abs(t.sicaklik-es.medyan)>5*es.dss) OR (abs(t.nem-en.medyan)>5*en.dss) OR (abs(t.titresim-et.medyan)>5*et.dss) anomali
FROM t
JOIN e es ON es.bolge=t.bolge AND es.saat=hour(t.zaman) AND es.olcu='sicaklik'
JOIN e en ON en.bolge=t.bolge AND en.saat=hour(t.zaman) AND en.olcu='nem'
JOIN e et ON et.bolge=t.bolge AND et.saat=hour(t.zaman) AND et.olcu='titresim'""")
def yaz(b, q):
    print(f"\n## {b}"); r = con.sql(q); print(" | ".join(r.columns))
    for x in r.fetchall(): print(" | ".join(str(v) for v in x))
yaz("Sensör başına ölçüm sayısı ve anomali oranı (%) — dağılım", """
WITH s AS (SELECT sensor_id, model, count(*) n, 100*avg(anomali::INT) oran FROM a GROUP BY ALL)
SELECT count(*) sensor, min(n) n_min, max(n) n_max, round(min(oran),3) oran_min, round(quantile_cont(oran,0.25),3) p25,
  round(median(oran),3) medyan, round(quantile_cont(oran,0.75),3) p75, round(max(oran),3) oran_max,
  count(*) FILTER (WHERE oran=0) hic_anomalisi_olmayan FROM s""")
yaz("Oran histogramı (%0,1'lik kovalar) ve modeller", """
WITH s AS (SELECT sensor_id, model, 100*avg(anomali::INT) oran FROM a GROUP BY ALL)
SELECT floor(oran*10)/10 kova, count(*) sensor, string_agg(DISTINCT model, ',') modeller FROM s GROUP BY 1 ORDER BY 1""")
yaz("Model bazında sensör oranlarının aralığı (%)", """
WITH s AS (SELECT sensor_id, model, 100*avg(anomali::INT) oran FROM a GROUP BY ALL)
SELECT model, count(*) sensor, round(min(oran),3) en_dusuk, round(median(oran),3) medyan, round(max(oran),3) en_yuksek FROM s GROUP BY model ORDER BY model""")
yaz("Sıçramalar kümeli mi? Anomaliden sonraki ölçüm de anomali mi? (sensör içinde zamana göre)", """
WITH x AS (SELECT sensor_id, model, anomali, lead(anomali) OVER (PARTITION BY sensor_id ORDER BY zaman) sonraki FROM a)
SELECT model, round(100*avg(sonraki::INT) FILTER (WHERE anomali),2) anomali_sonrasi_anomali_yuzde,
       round(100*avg(sonraki::INT) FILTER (WHERE NOT anomali),3) normal_sonrasi_anomali_yuzde FROM x GROUP BY model ORDER BY model""")
