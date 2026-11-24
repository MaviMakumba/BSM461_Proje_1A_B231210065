# B — Karar Defteri (Akış hattı · Şablon 3 IoT)

> Sunumda "neden?" sorusunun cevabı burada. Her karar: **bağlam → alternatifler → seçim → gerekçe (ölçüm) → bedeli → hoca sorarsa**.
> Kural: ölçülmemiş sayı yok → `[ÖLÇÜLECEK: ne, nasıl]`. Seçimi Eren yapar; asistan alternatif + ölçüm planı önerir.
> Durum etiketleri: `AÇIK` (karar bekliyor) · `KARAR` (seçildi, ölçüm bekliyor) · `ÖLÇÜLDÜ` (sayıyla desteklendi)

## Senaryo kartı ve ana gerilim (bütün kararların kökü)

| Hız | Tepe | Uyarı | Geç veri | Kayıp sınırı | Saklama | Servis p99 |
|---|---|---|---|---|---|---|
| 2.000 ölçüm/s | ×2 → 4.000/s | **5 s** | **%15, ≤2 dk** | **≤ %0,1** | 30 gün | 2000 ms |

**Gerilim:** Olayların %15'i 2 dakikaya kadar geç geliyor; sayıma girmeyen pay %0,1'i geçmemeli → watermark
neredeyse 2 dk'yı kapsamalı. Ama uyarı 5 s içinde çıkmalı → 2 dk bekleyen bir pencere uyarıyı geciktirir.
İkisi aynı sorguda birlikte sağlanamaz; hangisinden nasıl ödün verdiğimizi deneyle göstereceğiz (B6).
Kitteki `is/akis_iskelet.py` yorumu "%5, 1–10 dk" der — bu genel bir örnek; **bizim kartımız %15, ≤2 dk**.
İskeletteki konsol trigger'ı 10 s → 5 s uyarı sınırıyla zaten çelişiyor (B3'te karar).

---

## Kararlar

### K-01 · Hocanın kit güncellemesini (`KAFKA_LOG_DIRS`) almak — `DOĞRULANDI` (B0)
- **Bağlam:** Kit 24 Kas'ta güncellendi (kit commit `bf6b716`). Önceden compose'da `kafka-veri` birimi bağlıydı ama
  Kafka verisini o dizine yazmıyordu → konteyner yeniden yaratılınca konular ve offset'ler gidiyordu.
- **Alternatifler:** (1) Güncellemeyi almak · (2) Eski hâli korumak.
- **Seçim:** (1).
- **Gerekçe:** Yeniden başlatma deneyi (B7) ve Kafka kırılma deneyi (B8) "Kafka yeniden başladığında veri duruyor mu"
  sorusuna dayanıyor; veri kalıcı değilse deney Kafka'yı değil kit hatasını ölçer.
- **Bedeli:** Ekipte herkes `docker compose up -d` ile Kafka'yı yeniden yaratmalı; o ana kadarki konular kaybolur.
- **Hoca sorarsa:** *"Kafka yeniden başlarsa veri nerede?"* → Konu günlükleri `kafka-veri` Docker biriminde
  (`/var/lib/kafka/data`); `docker compose down` sonrası durur, `down -v` siler. Tek düğüm, replika 1 →
  disk giderse veri gider (üretimde RF=3, min.insync=2).

### K-02 · S3 healthcheck `localhost` → `s3` (C'nin değişikliği) — `AÇIK`
- **Bağlam:** C commit `3dc12e7` ile `docker-compose.yml`'de SeaweedFS healthcheck adresini değiştirdi; neden yazılmamış.
- **Yapılacak:** C'ye nedenini sor (muhtemel: konteyner içinde `localhost` IPv6'ya çözülüp wget başarısız oluyor —
  **doğrulanmadı**). Gerçekten gerekiyorsa README "kitten farklar" bölümüne yaz; gerekmiyorsa geri al.

### K-03 · Dört soru (A1, A2, T1, T2) — toplu taraf `KARAR`, akış tarafı `AÇIK`
- **T1 (A, 24 Kas'ta gruba yazdı):** sıcaklık, nem ve titreşim ölçümlerinin **gün içinde bölgelere göre** değişimi.
- **T2 (A):** anomali ve veri kaybı oranlarının **sensör modeli ve kurulum yılına** göre karşılaştırması (`sensorler.csv` join).
- T1 ve T2 ayrı sonuç tabloları (`iot_t1`, `iot_t2`); C'nin grafikleri bu tablolardan.
- **A1 (öneri):** son 10 dk'da normal dışına çıkan sensör (eşik) · **A2 (öneri):** susan (veri göndermeyen) sensör.
- **Neden A1–A2 birbirinin tekrarı değil:** A1 *gelen* veride eşik aşımı; A2 *gelmeyen* veriyi tespit (durum/zaman aşımı).
- **Akış ↔ toplu tekrar riski:** T2 de "anomali" ve "veri kaybı" sayıyor. Fark: T2 geçmişin toplu oranı (model/yıl
  kırılımı), A1/A2 canlı uyarı (hangi sensör, şu an). **"Anomali" tanımını A ile aynı tutmalıyız** — yoksa
  hoca "akışta anomali dediğiniz ile toplu tablodaki anomali aynı mı?" diye sorar.
- **Netleşecek (B3/B4, veriden):** "normal dışı" tanımı (sabit eşik mi, sensöre/bölgeye göre mi), "susmak" kaç dakika?

---

## Bekleyen kararlar (bölüm geldikçe yukarıya taşınır)

| Kod | Karar | Bölüm | Ölçüm planı (kısa) |
|---|---|---|---|
| K-04 | `startingOffsets` earliest ↔ latest | B1/B5 | yeniden başlatmada nereden devam ediyor (checkpoint varken önemi?) |
| K-05 | Kafka bölüm sayısı | B2 | tepe 4.000/s ÷ bölüm başına ölçülen tüketim hızı |
| K-06 | Kafka anahtarı (`sensor_id`?) ve sıralama garantisi | B2 | bölüm başına mesaj sayısı dengesi |
| K-07 | `acks=all` + idempotent üretici | B2 | `--acks 1` / `--idempotent-kapali` ile hız + tekrar farkı |
| K-08 | Trigger süresi (5 s uyarı) | B3 | batch süresi < trigger mı; uçtan uca gecikme |
| K-09 | A1 yöntemi: olay bazlı filtre ↔ pencere | B3 | uyarı gecikmesi |
| K-10 | A2 yöntemi: durumlu (state) ↔ pencere + `sensorler.csv` | B4 | yanlış alarm (geç veri ↔ susma) |
| K-11 | Çıkış kipi (append/update) ve sink (Delta) | B5 | — |
| K-12 | Tekrar ayıklama anahtarı (`dropDuplicates` + watermark) | B5 | önce/sonra sayım |
| K-13 | Watermark süresi | B6 | kart oranı ↔ 0,3 → kaçırılan olay sayısı |
| K-14 | Teslim garantisi (en az bir kez ↔ tam bir kez) | B7 | yeniden başlatmada kayıp/çift sayım |

---

## B1 bulguları — Kafka keşfi (24 Kas, `--hiz 20 --sure 60`, kanıt: `kanit/b1/`, `kanit/olcum/B.csv`)

Betik: `is/olcum/b1_kafka_kesif.py` (konsol tüketici dökümünü sayar; üretici koduna bakılmadı).

| # | Bulgu (ölçülen) | Neyi etkiler |
|---|---|---|
| 1 | Mesaj alanları: `olay_zamani` (ISO metin, UTC `Z`), `sensor_id`, `bolge`, `sicaklik`, `nem`, `titresim` (sayılar float). **Olay kimliği (olay_id) YOK.** Kafka anahtarı = `sensor_id`. | şema (B3), tekrar ayıklama K-12 |
| 2 | Üretici "1.168 gönderildi", Kafka'da **1.171** mesaj → fark **3** = birebir aynı 3 tekrar | watermark deneyi sayımı (B6) |
| 3 | Tekrarlar **aynı bölümde, ardışık ofsette, aynı CreateTime** ile → üretici **uygulaması iki kez göndermiş**; idempotent üretici açıkken bile Kafka'ya girdi | K-07, K-12 (aşağıda) |
| 4 | **(sensor_id, olay_zamani) benzersiz değil:** 38 grupta aynı sensör + aynı milisaniye ama değerler farklı | K-12: tekrar anahtarı bu ikili OLAMAZ |
| 5 | Geç olay (>5 s): **178 / 1.171 = %15,20**; en büyük gecikme **119,8 s**; 5–30 s arası hiç yok; 30–60 s: 59, 60–90 s: 47, 90–120 s: 72 | kartla uyumlu (%15, ≤2 dk) → K-13 |
| 6 | **144 mesajda** aynı sensörün `olay_zamani` Kafka sırasında **geri gidiyor** | K-06: Kafka *gönderim* sırasını korur, *olay zamanı* sırasını değil |
| 7 | 1 mesajda `sensor_id` yok, Kafka anahtarı boş metin | kirli kayıt (C'nin Z10'u); A1/A2'de filtre/karantina |
| 8 | 3 bölüm (kit varsayılanı), 200 farklı sensör, dağılım 402/351/418 → en dolu ÷ ortalama **1,071** | K-05/K-06 ön bilgi (1 dk'lık küçük örnek) |
| 9 | Kafka yeni açıldığında üretici ilk saniyede `Coordinator load in progress: retrying` uyarısı verdi, sonra devam etti | README notu / B8 |

**Hoca sorarsa (B1'den):**
- *"Idempotent üretici açıkken neden hâlâ tekrar var?"* → Idempotent üretici yalnızca **ağ/yeniden deneme**
  kaynaklı çift yazmayı önler (aynı üretici oturumu + sıra numarası). Uygulama aynı olayı **iki ayrı `send`** ile
  gönderirse Kafka bunları iki farklı mesaj görür. Kanıt: tekrarlar ardışık ofsette, ayrı mesaj olarak duruyor.
  Bu yüzden tekrar ayıklama **tüketici tarafında** (Spark) yapılmalı.
- *"Tekrarı hangi alanlarla ayıkladınız?"* → Olay kimliği yok; (sensor_id, olay_zamani) 38 grupta çakışıyor →
  tüm alanların birleşimi gerekli (K-12, B5'te karar).
- *"Kafka sırayı garanti ediyor mu?"* → Yalnızca aynı bölüm içinde, **yazılma** sırasını. Olay zamanı sırası değil:
  144 mesajda geri gitti. Olay zamanı sırasını watermark + pencere ile Spark kurar.

---

## Önemli gözlemler (karar değil ama sorulabilir)
- 2026-11-24 · Kit akış iskeleti trigger'ı konsolda 10 s, S3'te 30 s → kartın 5 s uyarısını karşılamaz.
- 2026-11-24 · Kitte tek Kafka düğümü, replika 1 (`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`) → Z0 garanti tablosunda "kopya: 1, üretimde 3" satırı.

## Soru bankası (prova için — her karar eklendikçe büyür)
1. Kafka konunuz kaç bölümlü, neden? → K-05
2. Anahtar neden `sensor_id`; sıralama nerede garanti? → K-06
3. Watermark kaç, kartın hangi sayısından? Kaç olay kaçırdınız? → K-13
4. 5 s uyarıyı 2 dk geç veriyle nasıl bağdaştırdınız? → K-08, K-09
5. Ağ kopan sensör ile susan sensörü nasıl ayırıyorsunuz? → K-10
6. Teslim garantiniz ne, nasıl kanıtladınız? → K-14
7. `acks=all` ne sağlıyor, bedeli ne? → K-07
8. Kafka çökerse ne oluyor? → B8
9. Idempotent üretici açıkken neden tekrar var? → B1 bulgu 3
10. Tekrarları hangi alanla ayıkladınız; olay kimliği var mı? → B1 bulgu 4, K-12
11. Kafka sıra garantisi olay zamanı sırası mı? → B1 bulgu 6
