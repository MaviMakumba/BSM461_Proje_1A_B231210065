# -*- coding: utf-8 -*-
"""B3 / K-09d — Arıza atağının VERİDEN ölçülen özellikleri (enjeksiyon aracının girdisi).

Çalıştırma: docker compose exec araclar python is/olcum/b3_atak_sapma.py
Girdi : toplu ham veri (K-15: --son-gun 2026-11-23) + eşik tablosu (K-09a stub) + sensorler.csv
Çıktı : kanit/b3/atak-ozet.txt          → her günün atak saati, atakta anomali payı, sapma dağılımı
        kanit/b3/atak-sapmalari.csv     → atak saatindeki ANORMAL ölçümlerin medyandan farkları (enjeksiyon bunlardan örnekler)
Tanımlar:
  arızalı grup  = toplu veride anomali oranı en yüksek 10 sensör (b3_ariza_zamani.py ile bulundu: sensor-050…059)
  atak saati    = her gün, arızalı grubun anomali oranının en yüksek olduğu UTC saat
  anomali       = K-09a: herhangi bir ölçü |değer − medyan| > 5 × dss (bölge × UTC saat)
  sapma         = değer − bölge×saat medyanı (ölçü başına, ham birimde)
"""
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='s3:8333'; SET s3_use_ssl=false; SET s3_url_style='path';"
            "SET s3_access_key_id='bsm461'; SET s3_secret_access_key='bsm461-gizli'; SET s3_region='us-east-1';")
ARIZALI = [f"sensor-{i:03d}" for i in range(50, 60)]

con.execute("""
CREATE TABLE t AS
SELECT h.sensor_id, s.bolge, TRY_CAST(h.olay_zamani AS TIMESTAMP) z, h.sicaklik, h.nem, h.titresim
FROM (SELECT DISTINCT * FROM read_json('s3://ham/iot/tarih=*/*.jsonl', format='newline_delimited',
      columns={'olay_zamani':'VARCHAR','sensor_id':'VARCHAR','sicaklik':'DOUBLE','nem':'DOUBLE','titresim':'DOUBLE'})) h
JOIN read_csv('s3://ham/boyut/iot/sensorler.csv', header=true) s USING (sensor_id)
WHERE h.sicaklik IS NOT NULL OR h.nem IS NOT NULL OR h.titresim IS NOT NULL""")
con.execute("""
CREATE TABLE e AS
SELECT bolge, saat,
  max(medyan) FILTER (WHERE olcu='sicaklik') ms, max(dss) FILTER (WHERE olcu='sicaklik') ds,
  max(medyan) FILTER (WHERE olcu='nem') mn,      max(dss) FILTER (WHERE olcu='nem') dn,
  max(medyan) FILTER (WHERE olcu='titresim') mt, max(dss) FILTER (WHERE olcu='titresim') dt
FROM 's3://islenmis/_stub/iot_esik.parquet' GROUP BY ALL""")
con.execute("""
CREATE TABLE a AS
SELECT t.*, CAST(z AS DATE) gun, hour(z) saat,
  t.sicaklik - e.ms d_sicaklik, t.nem - e.mn d_nem, t.titresim - e.mt d_titresim,
  coalesce(abs(t.sicaklik-e.ms) > 5*e.ds, false) OR coalesce(abs(t.nem-e.mn) > 5*e.dn, false)
    OR coalesce(abs(t.titresim-e.mt) > 5*e.dt, false) AS anomali
FROM t JOIN e ON t.bolge = e.bolge AND hour(t.z) = e.saat""")

con.execute(f"""
CREATE TABLE atak AS
SELECT gun, arg_max(saat, oran) saat, max(oran) oran FROM (
  SELECT gun, saat, 100*avg(anomali::INT) oran FROM a WHERE sensor_id IN {tuple(ARIZALI)} GROUP BY ALL) GROUP BY gun""")

with open("/kit/kanit/b3/atak-ozet.txt", "w", encoding="utf-8") as f:
    def yaz(baslik, sql):
        r = con.sql(sql)
        f.write(f"\n## {baslik}\n" + " | ".join(r.columns) + "\n")
        for satir in r.fetchall():
            f.write(" | ".join(str(x) for x in satir) + "\n")
    f.write("# Arıza atağının veriden ölçülen özellikleri (K-09d girdisi) · toplu veri --son-gun 2026-11-23 · eşik K-09a stub\n")
    yaz("Günlük atak saati (UTC) ve arızalı 10 sensörün o saatteki anomali payı (%)",
        "SELECT gun, saat AS atak_saati_utc, round(oran,2) anomali_payi FROM atak ORDER BY gun")
    yaz("Atak saatinde, sensör başına anomali payı (%) — dağılım (14 gün × 10 sensör)", f"""
        SELECT count(*) dilim, round(min(p),1) en_az, round(quantile_cont(p,0.25),1) c1, round(median(p),1) medyan,
               round(quantile_cont(p,0.75),1) c3, round(max(p),1) en_cok, round(avg(n),1) dilim_basina_olcum FROM (
          SELECT a.gun, sensor_id, 100*avg(anomali::INT) p, count(*) n FROM a JOIN atak USING (gun, saat)
          WHERE sensor_id IN {tuple(ARIZALI)} GROUP BY ALL)""")
    yaz("Atak saatindeki ANORMAL ölçümlerin medyandan farkı (ham birim)", f"""
        SELECT 'sicaklik' olcu, round(quantile_cont(d_sicaklik,0.05),2) y5, round(median(d_sicaklik),2) medyan, round(quantile_cont(d_sicaklik,0.95),2) y95 FROM a JOIN atak USING (gun,saat) WHERE anomali AND sensor_id IN {tuple(ARIZALI)}
        UNION ALL SELECT 'nem', round(quantile_cont(d_nem,0.05),2), round(median(d_nem),2), round(quantile_cont(d_nem,0.95),2) FROM a JOIN atak USING (gun,saat) WHERE anomali AND sensor_id IN {tuple(ARIZALI)}
        UNION ALL SELECT 'titresim', round(quantile_cont(d_titresim,0.05),3), round(median(d_titresim),3), round(quantile_cont(d_titresim,0.95),3) FROM a JOIN atak USING (gun,saat) WHERE anomali AND sensor_id IN {tuple(ARIZALI)}""")
    yaz("Atak saatinde aynı sensörlerin NORMAL ölçümlerinin medyandan farkı (kıyas)", f"""
        SELECT round(median(d_sicaklik),2) sicaklik, round(median(d_nem),2) nem, round(median(d_titresim),3) titresim, count(*) olcum
        FROM a JOIN atak USING (gun,saat) WHERE NOT anomali AND sensor_id IN {tuple(ARIZALI)}""")

con.execute(f"""
COPY (SELECT a.gun, a.saat, sensor_id, round(d_sicaklik,2) d_sicaklik, round(d_nem,1) d_nem, round(d_titresim,3) d_titresim
      FROM a JOIN atak USING (gun, saat) WHERE anomali AND sensor_id IN {tuple(ARIZALI)} ORDER BY ALL)
TO '/kit/kanit/b3/atak-sapmalari.csv' (HEADER)""")
print(open("/kit/kanit/b3/atak-ozet.txt", encoding="utf-8").read())
print("satır:", con.sql("SELECT count(*) FROM '/kit/kanit/b3/atak-sapmalari.csv'").fetchone()[0])
