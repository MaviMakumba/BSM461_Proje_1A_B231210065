# -*- coding: utf-8 -*-
"""B3 — "Normal dışı" tanımı için VERİDEN dağılım analizi (K-09a; A'nın T2'siyle ortak eşik).

Çalıştırma: docker compose exec araclar python is/olcum/b3_anomali_kesif.py
Girdi: s3://ham/iot/tarih=*/ (toplu veri, 3M olay / 14 gün) + s3://ham/boyut/iot/sensorler.csv
Soru: Ölçümler bölgeye mi, sensöre mi, modele mi göre değişiyor? Üç eşik seçeneğinde (genel / bölge / sensör)
kaç anomali çıkıyor ve sensör bazlı eşik, modeller arası farkı (T2) siliyor mu?
Eşik biçimi her seçenekte aynı: ortalama ± 3 standart sapma (yalnızca karşılaştırma için; kesin kural A ile seçilecek).
"""
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='s3:8333'; SET s3_use_ssl=false; SET s3_url_style='path';"
            "SET s3_access_key_id='bsm461'; SET s3_secret_access_key='bsm461-gizli'; SET s3_region='us-east-1';")

# Ham veriyi metin olarak oku → tip hatalarını da sayabilelim
con.execute("""
CREATE TABLE ham AS SELECT * FROM read_json('s3://ham/iot/tarih=*/*.jsonl', format='newline_delimited',
  columns={'olay_zamani':'VARCHAR','sensor_id':'VARCHAR','bolge':'VARCHAR',
           'sicaklik':'VARCHAR','nem':'VARCHAR','titresim':'VARCHAR'}, ignore_errors=true)""")
con.execute("CREATE TABLE sensor AS SELECT * FROM read_csv('s3://ham/boyut/iot/sensorler.csv', header=true)")

def yaz(baslik, sql):
    print(f"\n## {baslik}")
    r = con.sql(sql)
    print(" | ".join(r.columns))
    for satir in r.fetchall():
        print(" | ".join(str(x) for x in satir))

yaz("Satır ve kirli kayıt sayıları", """
SELECT count(*) AS satir,
  count(*) - count(DISTINCT (olay_zamani, sensor_id, bolge, sicaklik, nem, titresim)) AS birebir_tekrar_fazlasi,
  count(*) FILTER (WHERE sensor_id IS NULL OR sensor_id='') AS sensor_eksik,
  count(*) FILTER (WHERE bolge IS NULL OR bolge='') AS bolge_eksik,
  count(*) FILTER (WHERE sicaklik IS NOT NULL AND TRY_CAST(sicaklik AS DOUBLE) IS NULL) AS sicaklik_sayi_degil,
  count(*) FILTER (WHERE sicaklik IS NULL) AS sicaklik_eksik,
  count(*) FILTER (WHERE nem IS NULL) AS nem_eksik,
  count(*) FILTER (WHERE titresim IS NULL) AS titresim_eksik,
  count(*) FILTER (WHERE TRY_CAST(olay_zamani AS TIMESTAMP) IS NULL) AS zaman_bozuk
FROM ham""")

# Temiz görünüm: tekrarlar ayıklanmış, sayılar DOUBLE, sensör + boyut bilgisi eklenmiş
con.execute("""
CREATE TABLE t AS
SELECT DISTINCT h.sensor_id, s.bolge, s.model, s.kurulum_yili,
  TRY_CAST(h.olay_zamani AS TIMESTAMP) AS zaman,
  TRY_CAST(h.sicaklik AS DOUBLE) AS sicaklik, TRY_CAST(h.nem AS DOUBLE) AS nem, TRY_CAST(h.titresim AS DOUBLE) AS titresim
FROM ham h JOIN sensor s USING (sensor_id)""")

for o in ["sicaklik", "nem", "titresim"]:
    yaz(f"{o}: genel dağılım", f"""
    SELECT count({o}) n, round(min({o}),3) min, round(quantile_cont({o},0.001),3) p0_1, round(quantile_cont({o},0.01),3) p1,
      round(median({o}),3) p50, round(quantile_cont({o},0.99),3) p99, round(quantile_cont({o},0.999),3) p99_9,
      round(max({o}),3) max, round(avg({o}),3) ort, round(stddev({o}),3) ss FROM t""")
    yaz(f"{o}: bölgeye göre", f"""
    SELECT bolge, count({o}) n, round(avg({o}),2) ort, round(stddev({o}),3) ss,
      round(quantile_cont({o},0.01),2) p1, round(quantile_cont({o},0.99),2) p99, round(max({o}),2) max
    FROM t GROUP BY bolge ORDER BY ort""")
    yaz(f"{o}: modele göre", f"""
    SELECT model, count(DISTINCT sensor_id) sensor, round(avg({o}),2) ort, round(stddev({o}),3) ss,
      round(quantile_cont({o},0.99),2) p99, round(max({o}),2) max FROM t GROUP BY model ORDER BY model""")
    yaz(f"{o}: sensör farkı bölge içinde ne kadar? (bölge içi sensör ortalamalarının ss'si ↔ sensör içi ss)", f"""
    WITH s AS (SELECT bolge, sensor_id, avg({o}) ort, stddev({o}) ss FROM t GROUP BY ALL)
    SELECT bolge, count(*) sensor, round(stddev(ort),3) sensor_ortalamalari_ss, round(avg(ss),3) sensor_ici_ort_ss,
      round(min(ss),3) en_sakin_sensor_ss, round(max(ss),3) en_oynak_sensor_ss FROM s GROUP BY bolge ORDER BY bolge""")

# Üç eşik seçeneği: ort ± 3 ss — genel / bölge / sensör
con.execute("""
CREATE TABLE esik AS
WITH g AS (SELECT avg(sicaklik) gs, stddev(sicaklik) gss, avg(nem) gn, stddev(nem) gnss, avg(titresim) gt, stddev(titresim) gtss FROM t),
b AS (SELECT bolge, avg(sicaklik) bs, stddev(sicaklik) bss, avg(nem) bn, stddev(nem) bnss, avg(titresim) bt, stddev(titresim) btss FROM t GROUP BY bolge),
s AS (SELECT sensor_id, avg(sicaklik) ss_, stddev(sicaklik) sss, avg(nem) sn, stddev(nem) snss, avg(titresim) st, stddev(titresim) stss FROM t GROUP BY sensor_id)
SELECT t.*,
  (abs(sicaklik-gs)>3*gss OR abs(nem-gn)>3*gnss OR abs(titresim-gt)>3*gtss) AS a_genel,
  (abs(sicaklik-bs)>3*bss OR abs(nem-bn)>3*bnss OR abs(titresim-bt)>3*btss) AS a_bolge,
  (abs(sicaklik-ss_)>3*sss OR abs(nem-sn)>3*snss OR abs(titresim-st)>3*stss) AS a_sensor
FROM t, g JOIN b ON true JOIN s ON true WHERE b.bolge=t.bolge AND s.sensor_id=t.sensor_id""")

yaz("Üç seçenekte toplam anomali oranı (%)", """
SELECT count(*) n, round(100*avg(a_genel::INT),3) genel, round(100*avg(a_bolge::INT),3) bolge, round(100*avg(a_sensor::INT),3) sensor FROM esik""")
yaz("Hangi ölçü tetikliyor? (bölge eşiğiyle, %)", """
WITH b AS (SELECT bolge, avg(sicaklik) bs, stddev(sicaklik) bss, avg(nem) bn, stddev(nem) bnss, avg(titresim) bt, stddev(titresim) btss FROM t GROUP BY bolge)
SELECT round(100*avg((abs(sicaklik-bs)>3*bss)::INT),3) sicaklik, round(100*avg((abs(nem-bn)>3*bnss)::INT),3) nem,
       round(100*avg((abs(titresim-bt)>3*btss)::INT),3) titresim FROM t JOIN b USING (bolge)""")
yaz("T2 tuzağı: modele göre anomali oranı (%) — sensör eşiği farkı siliyor mu?", """
SELECT model, count(DISTINCT sensor_id) sensor, round(100*avg(a_genel::INT),3) genel, round(100*avg(a_bolge::INT),3) bolge,
  round(100*avg(a_sensor::INT),3) sensor_esigi FROM esik GROUP BY model ORDER BY model""")
yaz("T2 tuzağı: kurulum yılına göre anomali oranı (%)", """
SELECT kurulum_yili, count(DISTINCT sensor_id) sensor, round(100*avg(a_genel::INT),3) genel, round(100*avg(a_bolge::INT),3) bolge,
  round(100*avg(a_sensor::INT),3) sensor_esigi FROM esik GROUP BY kurulum_yili ORDER BY kurulum_yili""")
yaz("Sensör başına anomali oranlarının yayılımı (%) — sensör eşiğinde herkes aynı mı?", """
WITH s AS (SELECT sensor_id, 100*avg(a_bolge::INT) b, 100*avg(a_sensor::INT) s FROM esik GROUP BY sensor_id)
SELECT round(min(b),3) bolge_min, round(median(b),3) bolge_medyan, round(max(b),3) bolge_max, round(stddev(b),3) bolge_ss,
       round(min(s),3) sensor_min, round(median(s),3) sensor_medyan, round(max(s),3) sensor_max, round(stddev(s),3) sensor_ss FROM s""")
yaz("Bölge eşiğiyle en çok anomali üreten 10 sensör", """
SELECT sensor_id, bolge, model, kurulum_yili, count(*) n, round(100*avg(a_bolge::INT),3) bolge_oran, round(100*avg(a_sensor::INT),3) sensor_oran
FROM esik GROUP BY ALL ORDER BY bolge_oran DESC LIMIT 10""")

# ── Eşik katsayısı k'ya göre: gürültü (yanlış alarm) mı, gerçek sıçrama mı? ──
# Bölge bazlı iki istatistik: ortalama ± k·ss  ↔  dirençli: medyan ± k·1.4826·MAD (uç değerlerden etkilenmez)
con.execute("""
CREATE TABLE bi AS SELECT bolge,
  avg(sicaklik) s_o, stddev(sicaklik) s_s, median(sicaklik) s_m, 1.4826*mad(sicaklik) s_d,
  avg(nem) n_o, stddev(nem) n_s, median(nem) n_m, 1.4826*mad(nem) n_d,
  avg(titresim) t_o, stddev(titresim) t_s, median(titresim) t_m, 1.4826*mad(titresim) t_d FROM t GROUP BY bolge""")
yaz("Bölge istatistikleri: ortalama/ss ↔ medyan/dirençli ss (MAD·1.4826)", """
SELECT bolge, round(s_o,2) s_ort, round(s_s,3) s_ss, round(s_m,2) s_med, round(s_d,3) s_dss,
  round(n_s,3) n_ss, round(n_d,3) n_dss, round(t_s,3) t_ss, round(t_d,3) t_dss FROM bi ORDER BY bolge""")
for k in [3, 4, 5, 6]:
    yaz(f"k={k}: ölçü başına anomali oranı (%) — klasik (ort±k·ss) ↔ dirençli (med±k·dss)", f"""
    SELECT round(100*avg((abs(sicaklik-s_o)>{k}*s_s)::INT),4) s_klasik, round(100*avg((abs(sicaklik-s_m)>{k}*s_d)::INT),4) s_direncli,
           round(100*avg((abs(nem-n_o)>{k}*n_s)::INT),4) n_klasik,      round(100*avg((abs(nem-n_m)>{k}*n_d)::INT),4) n_direncli,
           round(100*avg((abs(titresim-t_o)>{k}*t_s)::INT),4) t_klasik, round(100*avg((abs(titresim-t_m)>{k}*t_d)::INT),4) t_direncli,
           round(100*avg((abs(sicaklik-s_m)>{k}*s_d OR abs(nem-n_m)>{k}*n_d OR abs(titresim-t_m)>{k}*t_d)::INT),4) herhangi_direncli
    FROM t JOIN bi USING (bolge)""")
yaz("Dirençli k=5 ile modele göre anomali oranı (%)", """
SELECT model, count(DISTINCT sensor_id) sensor,
  round(100*avg((abs(sicaklik-s_m)>5*s_d)::INT),4) sicaklik, round(100*avg((abs(nem-n_m)>5*n_d)::INT),4) nem,
  round(100*avg((abs(titresim-t_m)>5*t_d)::INT),4) titresim FROM t JOIN bi USING (bolge) GROUP BY model ORDER BY model""")
yaz("Sıçramaların büyüklüğü: bölge medyanından sapma, dirençli ss cinsinden (yalnızca >5 dss olanlar)", """
SELECT 'sicaklik' olcu, count(*) n, round(min(abs(sicaklik-s_m)/s_d),1) en_kucuk, round(median(abs(sicaklik-s_m)/s_d),1) medyan, round(max(abs(sicaklik-s_m)/s_d),1) en_buyuk
FROM t JOIN bi USING (bolge) WHERE abs(sicaklik-s_m)>5*s_d
UNION ALL
SELECT 'titresim', count(*), round(min(abs(titresim-t_m)/t_d),1), round(median(abs(titresim-t_m)/t_d),1), round(max(abs(titresim-t_m)/t_d),1)
FROM t JOIN bi USING (bolge) WHERE abs(titresim-t_m)>5*t_d""")

# ── Günlük döngü: sıcaklığın "normali" saate göre değişiyor mu? (bölge × saat tabanı) ──
con.execute("""
CREATE TABLE bs AS SELECT bolge, hour(zaman) saat, median(sicaklik) m, 1.4826*mad(sicaklik) d, count(*) n
FROM t WHERE sicaklik IS NOT NULL GROUP BY ALL""")
yaz("Sıcaklık: bölge içinde saatlik medyan aralığı (en soğuk saat ↔ en sıcak saat) ve bölge×saat dirençli ss", """
SELECT bolge, round(min(m),2) en_dusuk_saat_medyani, round(max(m),2) en_yuksek_saat_medyani, round(avg(d),3) bolge_saat_dss
FROM bs GROUP BY bolge ORDER BY bolge""")
for k in [3, 4, 5, 6]:
    yaz(f"k={k}: sıcaklık anomali oranı (%) — bölge tabanı ↔ bölge×saat tabanı (ikisi de dirençli)", f"""
    SELECT round(100*avg((abs(t.sicaklik-bi.s_m)>{k}*bi.s_d)::INT),4) bolge_tabani,
           round(100*avg((abs(t.sicaklik-bs.m)>{k}*bs.d)::INT),4) bolge_saat_tabani
    FROM t JOIN bi USING (bolge) JOIN bs ON bs.bolge=t.bolge AND bs.saat=hour(t.zaman)""")
yaz("Bölge×saat tabanı, k=5: modele göre sıcaklık anomali oranı (%)", """
SELECT model, round(100*avg((abs(t.sicaklik-bs.m)>5*bs.d)::INT),4) sicaklik
FROM t JOIN bs ON bs.bolge=t.bolge AND bs.saat=hour(t.zaman) GROUP BY model ORDER BY model""")
