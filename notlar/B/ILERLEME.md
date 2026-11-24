# B — İlerleme (Şablon 3 · IoT)

> **Şu an:** B3 — A1 normal dışı sensör · kural bitti, **enjeksiyon deneyi kaldı** · Son güncelleme: 2026-11-25
> Kapsam (Ş3): **Z8** akış hattı (10) · README + docker-compose (Z12'nin parçası) · canlı gösterim ·
> Z0 akış okları · Z11 ≥1 deney. (Z3 bu şablonda C'de.)
> Kararlar → [`B-karar-defteri.md`](B-karar-defteri.md) · Ölçümler → `kanit/olcum/B.csv`

| # | Bölüm | Çıktı | Hedef | Durum |
|---|---|---|---|---|
| B0 | Hazırlık | `rol-b` dalı, compose güncel, not dosyaları, README taslağı | 25 Kas | ⏳ yalnız README notu (K-02 cevabı) |
| B1 | Kafka keşfi | 20 olay incelemesi; geç + tekrar olay örneği (→ C) | 25 Kas | ✅ 24 Kas (K-04) |
| B2 | Konu tasarımı | bölüm sayısı hesabı, anahtar, acks/idempotent ölçümü | 27 Kas | ✅ 24 Kas (K-05/06/07) |
| B3 | A1 — anomali | konsolda çalışan sorgu (5 s uyarı) | 29 Kas | ⏳ kural + kod ✅ (K-08, K-09a–d); enjeksiyon deneyi ☐ |
| B4 | A2 — susan sensör | konsolda çalışan sorgu | 30 Kas | ☐ |
| B5 | Gerçek çıktı | Delta sink `akis_iot_a1/_a2` + checkpoint + tekrar ayıklama | **2 Ara** (C bekliyor) | ☐ |
| B6 | 🔴 Watermark deneyi | kart oranı ↔ `--gec-orani 0.3`; kaçırılan olay sayısı | 4 Ara | ☐ |
| B7 | Yeniden başlatma | kayıp / çift sayım sayıları → teslim garantisi | 5 Ara | ☐ |
| B8 | Kırılma (Z11) | Kafka durdurma ve/veya tepe yük; ağ kopması ↔ susma | 6 Ara | ☐ |
| B9 | Gösterim + README | 60–90 sn ekran kaydı; sıfırdan çalışan README; Z0 okları | 7 Ara | ☐ |
| B10 | Rapor | bölüm 8 (+3, 11 katkısı), YZ günlüğü, soru bankası | **8 Ara** | ☐ |

## ▶ Sonraki oturum buradan başlar (25 Kas sonu)
1. ✅ Duman testi (25 Kas, `--min-sicrama 12`, 40 s, 77.927 olay, 21 yığın, 200 sensör): hata yok, ⚠ 0, en yüksek sıçrama 5.
   Kanıt `kanit/b3/a1-k09c-duman.log`.
2. **Enjeksiyon aracı (K-09d):** `is/olcum/b3_ariza_enjeksiyonu.py` yaz.
   - Girdi: toplu veriden atak saatindeki sapmalar (`b3_ariza_zamani.py` ile bulunan atak saatleri; sapma = değer − bölge×saat medyanı).
   - Davranış: normal üretici `--hiz 2000` sürerken, belirli bir anda sensor-050…059 için ~10 ölçüm/s, %50'si gerçek sapma büyüklüğüyle
     Kafka `sensorler` konusuna basılır (anahtar sensor_id, K-06). Atak başlangıç zamanı kaydedilir.
   - Deney: atağı pencerenin **başında / ortasında / sonunda** başlat (oran seyrelmesi riski, K-09c). Ölç: tespit süresi
     (atak başı → ⚠), yanlış alarm. Sonuç → `B.csv`, `kanit/b3/enjeksiyon-*.txt`, defter K-09d, sunum §6.4–6.5.
3. Pencere sonunda tespit > 5 s çıkarsa **K-09e** kararı: kısa pencere (1–2 dk) / kayan pencere / son-X-ölçüm oranı → Eren'e sun.
4. B3'ü kapat → **B4 (A2 susan sensör)**: sensör başına gönderim aralığını veriden ölç, K-10.
5. ✅ 25 Kas: B0–B3 main'e alındı (PR #2, merge 67d243d); A ve C için notlar PR açıklamasında. Bundan sonra her bölüm sonunda rol-b → main PR + merge.

## B0 alt adımları
- [x] Depo klonlandı, `rol-b` dalı açıldı
- [x] Hocanın 24 Kas kit güncellemesi (`KAFKA_LOG_DIRS`) compose'a alındı → K-01
- [x] Karar defteri + ilerleme dosyası + YZ günlüğü açıldı
- [x] Kit ayağa kalktı (24 Kas): `dogrula.py` çekirdek ✅, `dogrula_spark.py` ✅ (Parquet, Delta, Kafka); Kafka `log.dirs=/var/lib/kafka/data` doğrulandı
- [ ] README'ye "kitten farklar" bölümü (K-01, K-02)
- [x] T1/T2 kesinleşti (A) → K-03 · [ ] A1/A2 ekibe önerildi, onay bekleniyor

## Ekibe iletilecekler
- **A:** PR #1 merge edildi. Notlar: (1) Parquet "temizleme sonrası" ölçülmüş, JSON ham → satır sayısı farklı,
  oran böyle savunulamaz; (2) 112 dosya, ort. ~0,40 MB → küçük dosya (Z4 malzemesi); (3) `makine` sütununa
  donanım yazılmalı (hostname değil); (4) CSV BOM'lu ve tırnaklı.
- **C:** (Ekip kararı 24 Kas: C main'e doğrudan push edebilir, onay gerekmez.) S3 healthcheck `localhost→s3`
  değişikliğinin nedeni? (README'ye yazılacak)
- **A:** A1/A2'deki "anomali" tanımını T2 ile ortak yapalım (K-03).
- **Herkes:** main'i çekip `docker compose up -d` (Kafka konteyneri yeniden yaratılır; mevcut konular silinir — zaten kalıcı değildi).
- **C (veri sözlüğü için hazır):** geç olay, tekrar ve kirli kayıt örnekleri → `kanit/b1/ilk-20-mesaj.txt`,
  `kanit/b1/kafka-kesif-ozet.txt`. Alan listesi ve tipleri de orada. Olay kimliği alanı yok.
- **A (B3'ten):** (1) eşik tablosunu `s3a://tablolar/iot_esik` olarak yazsın: (bolge, saat, olcu, medyan, dss), 504 satır, K-09a —
  o gelene kadar akış `s3://islenmis/_stub/iot_esik.parquet` stub'ını okuyor (`is/akis/esik_stub.py`). (2) T2 için: TX-200 diğer
  modellerden ~4 kat fazla anomali; arıza günde bir kez 10 sensörde ortak atak (`kanit/b3/ariza-zamani.txt`).
- **C (B3'ten):** kirli veri sayıları — 6.050 tekrar; eksik: sensor_id 1215, bolge 1225, sicaklik 1184, nem 1231, titresim 1199.
  K-02 düzeltmesi: SeaweedFS IPv4 dinliyor, `localhost` IPv6'ya çözülüyor (127.0.0.1 çalışıyor). A1 satırında 5 ortak sütuna ek
  `olcum_sayisi`, `anomali_sayisi` olacak.
