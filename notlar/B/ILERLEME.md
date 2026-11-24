# B — İlerleme (Şablon 3 · IoT)

> **Şu an:** B0 — Hazırlık · Son güncelleme: 2026-11-24
> Kapsam (Ş3): **Z8** akış hattı (10) · README + docker-compose (Z12'nin parçası) · canlı gösterim ·
> Z0 akış okları · Z11 ≥1 deney. (Z3 bu şablonda C'de.)
> Kararlar → [`B-karar-defteri.md`](B-karar-defteri.md) · Ölçümler → `kanit/olcum/B.csv`

| # | Bölüm | Çıktı | Hedef | Durum |
|---|---|---|---|---|
| B0 | Hazırlık | `rol-b` dalı, compose güncel, not dosyaları, README taslağı | 25 Kas | ⏳ |
| B1 | Kafka keşfi | 20 olay incelemesi; geç + tekrar olay örneği (→ C) | 25 Kas | ☐ |
| B2 | Konu tasarımı | bölüm sayısı hesabı, anahtar, acks/idempotent ölçümü | 27 Kas | ☐ |
| B3 | A1 — anomali | konsolda çalışan sorgu (5 s uyarı) | 29 Kas | ☐ |
| B4 | A2 — susan sensör | konsolda çalışan sorgu | 30 Kas | ☐ |
| B5 | Gerçek çıktı | Delta sink `akis_iot_a1/_a2` + checkpoint + tekrar ayıklama | **2 Ara** (C bekliyor) | ☐ |
| B6 | 🔴 Watermark deneyi | kart oranı ↔ `--gec-orani 0.3`; kaçırılan olay sayısı | 4 Ara | ☐ |
| B7 | Yeniden başlatma | kayıp / çift sayım sayıları → teslim garantisi | 5 Ara | ☐ |
| B8 | Kırılma (Z11) | Kafka durdurma ve/veya tepe yük; ağ kopması ↔ susma | 6 Ara | ☐ |
| B9 | Gösterim + README | 60–90 sn ekran kaydı; sıfırdan çalışan README; Z0 okları | 7 Ara | ☐ |
| B10 | Rapor | bölüm 8 (+3, 11 katkısı), YZ günlüğü, soru bankası | **8 Ara** | ☐ |

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
