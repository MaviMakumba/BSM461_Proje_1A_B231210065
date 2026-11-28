# YZ kullanım günlüğü — B (Eren Kartal)

Biçim: `tarih · araç · adım/bileşen · ne için · çıktının ne kadarı kullanıldı`

| Tarih | Araç | Adım / bileşen | Ne için | Ne kadarı kullanıldı |
|---|---|---|---|---|
| 2026-11-19 | Claude Code (Opus) | Faz 0 | Yönerge analizi, rol dağılımı, şablon karşılaştırması | Rol dosyaları ekiple gözden geçirilip kullanıldı |
| 2026-11-24 | Claude Code (Opus) | B0 / depo | PR #1 incelemesi ve çakışma çözümü, kit güncellemesinin (KAFKA_LOG_DIRS) alınması, bölüm planı ve karar defteri iskeleti | Plan ve iskelet olduğu gibi; kararlar Eren'e ait |
| 2026-11-24 | Claude Code (Opus) | B1–B2 / Z8 | Kafka keşif ve ölçüm betikleri (`is/olcum/b1_kafka_kesif.py`, `b2_bolum_kapasite.py`), `is/akis/konu_hazirla.py`, ölçüm planı ve alternatiflerin açıklanması | Betikler olduğu gibi; kararlar (K-04, K-05, K-06) Eren'in; K-07 gerekçe cümlesi asistan taslağı, Eren kabul etti |
| 2026-11-24/25 | Claude Code (Opus) | B3 / Z8 (A1) | Anomali tanımı için DuckDB analizleri (`b3_anomali_kesif.py`, `b3_sensor_orani.py`, `b3_ariza_zamani.py`), eşik stub'ı, A1 akış işi (`a1_anomali.py`), gecikme deneyi betiği ve çevrimdışı oran analizi; otomatik kod incelemesinin iki bulgusunun düzeltilmesi | Betikler olduğu gibi; kararlar (K-08, K-09a–d) Eren'in; K-08, K-09b, K-09c, K-09d gerekçe cümleleri asistan taslağı, Eren kabul etti |
| 2026-11-28 | Claude Code (Opus) | B5 hazırlık / ekip | C'nin PR #3–4 incelemesi; ekipteki veri farkının kök nedeni (üretim günü) ve sha256 doğrulaması; `uretici.py --son-gun` eklemesi; Delta düz Parquet okuma deneyi; eşik stub'ındaki tekrar ayıklama hatasının bulunup etkisinin ölçülmesi; README kitten farklar bölümü | Kod ve ölçümler olduğu gibi; K-15, K-16 gerekçe cümleleri asistan taslağı, Eren kabul etti |
| 2026-11-28 | Claude Code (Opus) | B3 / Z8 (A1) | Atak özelliklerinin toplu veriden ölçümü, arıza enjeksiyon aracı ve deney betiği, iki canlı deney (`enj1`, `enj2`), Kafka'dan geri okuma ile enjeksiyon doğrulaması, 7 kuralın çevrimdışı tekrar oynatma karşılaştırması, yanlış alarmın kök neden analizi, A1'de kayan pencere ve çoklu uyarı düzeltmesi; A'nın PR #6'sının bağımsız doğrulaması | Kod ve ölçümler olduğu gibi; K-09e seçimi Eren'in (seçenekler + ölçümle sunuldu), gerekçe cümlesi asistan taslağı, onay bekleniyor |
