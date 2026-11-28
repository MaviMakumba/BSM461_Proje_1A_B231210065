# -*- coding: utf-8 -*-
"""K-09a — A'nın eşik tablosu (`s3a://tablolar/iot_esik`, Delta) ile B'nin referans hesabı (stub) birebir aynı mı?

Çalıştırma: docker compose exec araclar python is/akis/esik_karsilastir.py
Referans : s3://islenmis/_stub/iot_esik.parquet (is/akis/esik_stub.py üretir)
Beklenen : 504 satır, aynı anahtarlar (bolge, saat, olcu), medyan ve dss farkı ~0 (yalnızca kayan nokta yuvarlaması, < 1e-9)
"""
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs; INSTALL delta; LOAD delta;")
con.execute("CREATE SECRET (TYPE s3, KEY_ID 'bsm461', SECRET 'bsm461-gizli', ENDPOINT 's3:8333', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
con.execute("CREATE TABLE a AS SELECT * FROM delta_scan('s3://tablolar/iot_esik')")
con.execute("CREATE TABLE b AS SELECT * FROM 's3://islenmis/_stub/iot_esik.parquet'")
print("A şeması:", [(c[0], c[1]) for c in con.sql("DESCRIBE a").fetchall()])
print("B şeması:", [(c[0], c[1]) for c in con.sql("DESCRIBE b").fetchall()])
na, nb = con.sql("SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b)").fetchone()
print(f"satır: A {na} · B {nb}")
r = con.sql("""SELECT count(*) eslesen,
  max(abs(a.medyan - b.medyan)) medyan_fark, max(abs(a.dss - b.dss)) dss_fark
FROM a JOIN b USING (bolge, saat, olcu)""").fetchone()
print(f"anahtarı eşleşen: {r[0]} · en büyük medyan farkı {r[1]} · en büyük dss farkı {r[2]}")
eksik = con.sql("SELECT bolge, saat, olcu FROM b EXCEPT SELECT bolge, saat, olcu FROM a").fetchall()
fazla = con.sql("SELECT bolge, saat, olcu FROM a EXCEPT SELECT bolge, saat, olcu FROM b").fetchall()
print(f"A'da eksik anahtar: {len(eksik)} {eksik[:5]} · A'da fazla anahtar: {len(fazla)} {fazla[:5]}")
tamam = na == nb == r[0] == 504 and not eksik and not fazla and r[1] < 1e-9 and r[2] < 1e-9
print("SONUÇ:", "✓ birebir aynı" if tamam else "✗ fark var — yukarıya bak")
