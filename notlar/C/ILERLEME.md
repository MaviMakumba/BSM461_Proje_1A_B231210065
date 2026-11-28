# C — İlerleme (Şablon 3 · IoT)

> **Durum:** Üye C teknik görevleri (Adım 3, Z1, Z3, Z6, Z10, Pano/API) tamamlandı ve `rol-c` / PR #3 ile GitHub'a gönderildi.
> Kararlar → [`C-karar-defteri.md`](C-karar-defteri.md) · Kanıtlar → `kanit/c_*.txt` ve `kanit/c_pano.png`

| # | Bölüm | Çıktı | Kanıt | Durum |
|---|---|---|---|---|
| C0 | Kurulum & Healthcheck | `rol-c` dalı, S3 healthcheck düzeltmesi (`localhost` → `s3`), MongoDB profili | `docker-compose.yml` | ✅ Tamamlandı |
| C1 | Veri Tanıma & Ölçek (Z1) | Veri sözlüğü, şema kontrolü, 30 günlük kapasite ve 10×/100× projeksiyonu | `kanit/c_tani_olcek_kirli.txt` | ✅ Tamamlandı |
| C2 | Kirli Veri & Parquet (Z10) | Spark temizleme, karantina, boyut zenginleştirme, Parquet sıkıştırma ölçümü | `kanit/c_z10_temizleme_ozeti.txt` | ✅ Tamamlandı |
| C3 | Sıcak Nokta Analizi (Z3) | Spark çarpıklık (1.03×) ve tuzlamalı/tuzlamasız süre karşılaştırması | `kanit/c_z3_sicak_nokta.txt` | ✅ Tamamlandı |
| C4 | Servis Deposu (Z6) | MongoDB 7.0 koleksiyonları, bileşik indeksler ve 1.000 sorguda `p99 = 0.46 ms` testi | `kanit/c_z6_servis_gecikme.txt` | ✅ Tamamlandı |
| C5 | Canlı Pano & REST API | `http://localhost:8000` web arayüzü, bölge filtresi ve S3 akış senkronizasyonu | `kanit/c_pano.png` | ✅ Tamamlandı |
| C6 | B5 Entegrasyon & Rapor | B5 `akis_iot_a1/_a2` canlı testi, final raporu Üye C bölümü ve sunum provası | `rapor/yz-gunlugu-C.md` | ⏳ B5 bekleniyor |
