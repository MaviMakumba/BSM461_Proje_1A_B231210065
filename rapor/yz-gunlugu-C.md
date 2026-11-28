# YZ kullanım günlüğü — C (Ali Halit Örnek)

Biçim: `tarih · araç · adım/bileşen · ne için · çıktının ne kadarı kullanıldı`

| Tarih | Araç | Adım / bileşen | Ne için | Ne kadar kullanıldı |
|---|---|---|---|---|
| 2026-11-24 | Gemini | C0 / Altyapı | `docker-compose.yml` içindeki SeaweedFS S3 healthcheck IPv6 `localhost` sorununun teşhisi ve `s3:9333` olarak düzeltilmesi | Teşhis ve tek satırlık compose düzeltmesi uygulandı |
| 2026-11-26 | Gemini | C1 / Adım 3 & Z1 | DuckDB ile S3 ham JSONL şema analizi, kirli veri sayımı ve 30 günlük kapasite/bant genişliği hesabı betiği (`analiz/c_olcumler.py`, `analiz/z10_detay.py`) | Betikler çalıştırılıp tüm sayılar `kanit/c_tani_olcek_kirli.txt` dosyasına doğrulandı |
| 2026-11-27 | Gemini | C2–C3 / Z10 & Z3 | Spark ile tekrar/eksik kayıt karantina ve Parquet hattı (`is/z10_temizle.py`) ile tuzlamasız/tuzlamalı sıcak nokta ölçüm betiği (`is/z3_sicak_nokta.py`); `.hint()` ve `tarih` bölümleme hatalarının giderilmesi | Betikler yerel Spark kümesinde çalıştırıldı; kararlar ve yorumlar gözden geçirildi |
| 2026-11-27 | Gemini | C4–C5 / Z6 & Pano | MongoDB 7.0 veri yükleme, indeksleme ve 1.000 sorguluk p50/p95/p99 gecikme testi (`servis/z6_mongo_yukle_olcum.py`) ile Python `ThreadingHTTPServer` tabanlı REST API ve Web Panosu (`servis/pano_api.py`) | Kodlar çalıştırılıp `p99 = 0.46 ms` ve pano arayüzü doğrulandı |
| 2026-11-27 | Gemini | C6 / Belgeleme | `notlar/C/` karar defteri, ilerleme tablosu, sunum notları ve YZ kullanım günlüğü taslaklarının oluşturulması | Ölçüm kanıtlarıyla birebir kontrol edilerek projeye eklendi |
