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
| B3 | A1 — anomali | konsolda çalışan sorgu (5 s uyarı) | 29 Kas | ✅ 28 Kas (K-08, K-09a–e; enjeksiyon 40/40, en çok 3,67 s, 0 yanlış alarm) |
| B4 | A2 — susan sensör | konsolda çalışan sorgu | 30 Kas | ☐ |
| B5 | Gerçek çıktı | Delta sink `akis_iot_a1/_a2` + checkpoint + tekrar ayıklama | **2 Ara** (C bekliyor) | ☐ |
| B6 | 🔴 Watermark deneyi | kart oranı ↔ `--gec-orani 0.3`; kaçırılan olay sayısı | 4 Ara | ☐ |
| B7 | Yeniden başlatma | kayıp / çift sayım sayıları → teslim garantisi | 5 Ara | ☐ |
| B8 | Kırılma (Z11) | Kafka durdurma ve/veya tepe yük; ağ kopması ↔ susma | 6 Ara | ☐ |
| B9 | Gösterim + README | 60–90 sn ekran kaydı; sıfırdan çalışan README; Z0 okları | 7 Ara | ☐ |
| B10 | Rapor | bölüm 8 (+3, 11 katkısı), YZ günlüğü, soru bankası | **8 Ara** | ☐ |

## ▶ Sonraki oturum buradan başlar (28 Kas sonu)
- **28 Kas akşam:** B3 KAPANDI. K-09d ölçüldü (`enj1`: eski kural 13/40 > 5 s, 1 yanlış alarm) → K-09e: 1 dk kayan pencere, ≥15 (`enj2`: 40/40,
  en çok 3,67 s, 0 yanlış alarm). Çoklu ⚠ hatası düzeltildi. A'nın PR #6'sı alındı (`iot_esik` birebir). `enj2` sırasında bellek baskısıyla arka plan
  komutu durduruldu (deney bitmişti, veri tam); Spark tek işte ~3 GB tutuyor → B5'te uzun koşularda belleğe dikkat.
- **Sıradaki:** B4 (A2 susan sensör, K-10): akış verisinden sensör başına gönderim aralığı ve susma süreleri ölçülecek. Sonra B5 (2 Ara).
0. **28 Kas yapılanlar:** C'nin PR #3–4'ü rol-b'ye alındı. Veri farkı çözüldü → **K-15** (`uretici.py --son-gun 2026-11-23`, sha256 14/14 aynı).
   **K-16** (A1/A2'ye `bolge`). Eşik stub'ı tekrar ayıklama hatası düzeltildi (etki ölçüldü, anomali sayısı değişmedi). README "Kitten farklarımız".
   Kanıt `kanit/b5/c-veri-farki.txt`. ✅ Eren onayladı; rol-b → main PR ile alındı. **Ekip mesajı** (aşağıda "28 Kas ekip mesajı") Eren tarafından gönderilecek.
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
- [x] README'ye "kitten farklar" bölümü (K-01, K-02, K-15, port 8000) — 28 Kas
- [x] T1/T2 kesinleşti (A) → K-03 · [ ] A1/A2 ekibe önerildi, onay bekleniyor

## Ekibe iletilecekler
- **A, PR #6 (28 Kas, merge c96aeb2):** `iot_esik` (Delta) B'de bağımsız doğrulandı → 504/504, fark 0,0 (`esik_karsilastir.py`). `iot_t1` 2.352 satır,
  `iot_t2` 39 satır. PR yorumunda istenenler: Z2 kanıtlarını K-15 verisiyle yenile, `z2-esit-kayit-boyut.txt` UTF-16 → UTF-8, Delta'yı `delta_scan` ile oku.
  A'nın sorusu (T2 veri_kaybi tabanı ↔ A2) → cevaplandı, A kabul etti (kod değişmiyor): raporda T2–A2 kapsam/hız farkı tek cümle;
  T2'de sağlıklı grupların ~%0,1–0,5 bandı "gürültü tabanı" olarak yazılacak. A2 eşiği B4'te (K-10) akış verisinden ölçülecek.
  **B5'te:** A1 `--esik` artık `s3a://tablolar/iot_esik` (Delta) okuyacak; stub yalnız yedek.
- **A'nın cevabı (28 Kas):** 23 Kas üretimi (6.030, C ile aynı). K-15'e geçip Z2'yi yeniden ölçecek; `iot_t1`, `iot_t2`, `iot_esik`
  hedefi **30 Kas**; 1 Ara'ya kadar gelmezse B yazar (A kabul). K-09a'yı onayladı. Ona şema + dss formülü + `is/akis/esik_karsilastir.py` gönderildi.
  ⚠ B5 notu: A1 şu an eşiği Parquet okuyor; A'nınki Delta → `--esik` okuma biçimi Delta'ya çevrilecek.
- **28 Kas ekip mesajı (gönderilecek):**
  - Herkese K-15 (veri `--son-gun 2026-11-23` ile yeniden üretilecek; kontrol: üretici "6.050 tekrar" yazmalı).
  - C'ye istekler: `delta_scan`, `ORDER BY`, `bolge` geliyor/`model` sende, anomali adı, hata günlüğü, p99 sabit yazı.
  - A'ya sorular: veri günü, `iot_esik` tarihi (1 Ara'ya kadar gelmezse B yazar), T2 tanımı.
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
