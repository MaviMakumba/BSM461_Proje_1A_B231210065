# C — Karar Defteri (Veri Tanıma, Temizleme, Sıcak Nokta ve Servis/Pano · Şablon 3 IoT)

> Her karar: **bağlam → alternatifler → seçim → gerekçe (ölçüm) → bedeli → hoca sorarsa**.
> Durum etiketleri: `KARAR` · `ÖLÇÜLDÜ` (kanıt dosyasındaki sayılarla doğrulandı)

## Senaryo Kartı ve Üye C Kapsamı

| Hız | Tepe | Uyarı | Geç veri | Kayıp sınırı | Saklama | Servis p99 |
|---|---|---|---|---|---|---|
| 2.000 ölçüm/s | ×2 → 4.000/s | 5 s | %15, ≤2 dk | ≤ %0,1 | **30 gün** | **≤ 2000 ms** |

Üye C sorumlulukları: **Adım 3 (Veri sözlüğü)** · **Z1 (Ölçek ve 30 günlük kapasite hesabı)** · **Z10 (Kirli veri temizleme, karantina ve Parquet katmanı)** · **Z3 (Sıcak nokta / skew ve tuzlama analizi)** · **Z6 (Servis deposu MongoDB + indeksleme + p99 gecikme testi)** · **Canlı Pano ve REST API (`:8000`)**.

---

## Kararlar

### K-C01 · S3 Healthcheck İsim Çözümlemesi (`localhost` → `s3`) — `ÖLÇÜLDÜ`
- **Bağlam:** Orijinal `docker-compose.yml` dosyasında `bsm461-s3` sağlık kontrolü `http://localhost:9333/cluster/status` adresini kullanıyordu; konteyner içinde `localhost` IPv6 (`::1`) adresine çözümlenirken SeaweedFS yalnızca IPv4 dinlediği için sağlık kontrolü başarısız oluyor ve bağımlı servisler (`s3-kur`, `spark`, `araclar`) başlamıyordu.
- **Alternatifler:** (1) `http://s3:9333/cluster/status` (Docker iç DNS) · (2) `http://127.0.0.1:9333/cluster/status` · (3) Orijinal ayarı korumak (çalışmıyor).
- **Seçim:** (1) Docker iç servis adı `s3` kullanıldı.
- **Gerekçe (Ölçüm):** `s3` servis adı doğrudan konteyner IPv4 adresine çözümlendi ve tüm çekirdek servisler (`kafka`, `s3`, `spark`, `araclar`) kesintisiz `Healthy` durumuna geçti (`dogrula.py` ile doğrulandı).
- **Bedeli:** Docker Compose ağı dışından tek başına çalıştırılırsa servis adı yerine IP adresi gerekir.

### K-C02 · Ham Veri Profilleme ve 30 Günlük Kapasite Planlaması (Adım 3 & Z1) — `ÖLÇÜLDÜ`
- **Bağlam:** S3 `ham` kovasında 14 günlük (`2026-11-09`..`2026-11-22`) JSONL verisi bulunuyor. Senaryo kartındaki 2.000 olay/s ortalama ve 4.000 olay/s tepe yük ile 30 günlük depolama/bant genişliği gereksinimi hesaplanmalıydı.
- **Seçim:** DuckDB (`httpfs` eklentisi) ile doğrudan S3 üzerinden şema, eksiklik, uç değer ve olay başı bayt ölçümü yapıldı (`analiz/c_olcumler.py`, kanıt: `kanit/c_tani_olcek_kirli.txt`).
- **Ölçülen Bulgular:**
  - Toplam ham kayıt: **3.000.000 satır** (14 JSONL dosyası, toplam **410.66 MB**, olay başı ortalama **143.54 bayt**).
  - Ortalama günlük hacim: **172.800.000 olay/gün** = **23.10 GB/gün** · Tepe bant genişliği (×2): **4.59 Mbps**.
  - 30 günlük ham kapasite: **5.184.000.000 olay** = **692.99 GB** (tek kopya) · **2.08 TB** (3 kopya / replikasyon).
  - Büyüme senaryoları: **10×** yükte **6.93 TB/30 gün** (3 kopya **20.79 TB**), **100×** yükte **69.30 TB/30 gün** (3 kopya **207.90 TB**).
- **Hoca sorarsa:** *"Veri sözlüğünde neden olay kimliği (event_id) yok?"* → Ham şemada 6 sütun (`olay_zamani`, `sensor_id`, `bolge`, `sicaklik`, `nem`, `titresim`) bulunuyor; tekil bir `event_id` alanı olmadığı için tekrar ayıklamada tam satır eşleşmesi (`dropDuplicates()`) kullanıldı.

### K-C03 · Kirli Veri Stratejisi: Karantina + Parquet Dönüşümü (Z10) — `ÖLÇÜLDÜ`
- **Bağlam:** Ham veride tam tekrar eden olaylar ve zorunlu alanları (`sensor_id`, `bolge`, `sicaklik`, `nem`, `titresim`) eksik kayıtlar tespit edildi. Bu kayıtların sessizce silinmesi izlenebilirliği bozar; ham JSONL biçiminde saklanması ise yüksek disk ve okuma maliyeti doğurur.
- **Alternatifler:** (1) Kirli kayıtları sessizce düşürmek · (2) Eksik alanları ortalama/medyan ile doldurmak (imputation) · **(3) Tam tekrarları ayıklamak, eksik/sınır dışı kayıtları ayrı bir `karantina` katmanına almak ve temiz veriyi `tarih` bölümlemeli Parquet olarak yazmak**.
- **Seçim:** (3) Karantina + Parquet (`is/z10_temizle.py`, kanıt: `kanit/c_z10_temizleme_ozeti.txt`).
- **Gerekçe (Ölçüm):**
  - 3.000.000 ham kayıttan **6.030 tam tekrar (%0.201)** ayıklandı; zorunlu alanı eksik **6.038 kayıt (%0.201)** `s3://islenmis/iot/karantina` altına alındı.
  - Kalan **2.987.932 (%99.598)** geçerli kayıt `sensorler.csv` boyut tablosuyla (`model`, `kurulum_yili`) zenginleştirilip `anomali_bayragi` (`9.277` uç değer, %0.310) eklenerek `s3://islenmis/iot/temiz` altına yazıldı.
  - **Biçim kazancı:** Ham JSONL **410.66 MB** (DuckDB okuma: **1.75 s**) → Temiz Parquet **77.14 MB** (DuckDB okuma: **0.19 s**) = **%81.2 disk tasarrufu** ve **9.2× daha hızlı analitik okuma**.
- **Bedeli:** Spark toplu temizleme ve Parquet yazma adımı tek seferlik **35.60 s** sürdü ve karantina klasörü için ek depolama alanı ayrıldı.
- **Hoca sorarsa:** *"Neden eksik sıcaklık/nem değerlerini ortalamayla doldurmadınız?"* → IoT anomali tespitinde yapay ortalama doldurma gerçek arıza sinyallerini bastırır ve sahte normallik üretir; bu nedenle bozuk kayıtları karantinaya ayırmak daha güvenlidir.

### K-C04 · Sıcak Nokta (Skew) Analizi ve Tuzlamama Kararı (Z3) — `ÖLÇÜLDÜ`
- **Bağlam:** `sensor_id` anahtarına göre yapılan kümeleme (`groupBy`) ve boyut birleştirme (`join`) işlemlerinde bazı sensörlerin aşırı yoğunlaşıp Spark görevlerini yavaşlatıp yavaşlatmadığı ölçüldü.
- **Alternatifler:** (1) Standart `groupBy + join` · (2) İki aşamalı tuzlama (`salting`, `N=10`).
- **Seçim:** Düşük çarpıklık nedeniyle standart yaklaşım ve küçük boyut tablosu için `BROADCAST` join tercih edildi (`is/z3_sicak_nokta.py`, kanıt: `kanit/c_z3_sicak_nokta.txt`).
- **Gerekçe (Ölçüm):**
  - 200 sensör arasında olay dağılımı: en az **10.774**, ortanca **15.373**, en çok **15.846** (`sensor-163`).
  - Çarpıklık oranı (en çok / ortanca): **1.03×** (en az olaya göre **1.47×**).
  - Spark 3.5 (AQE kapalı, 16 shuffle bölümü) üzerinde 3M satırda tuzlamasız süre **1.52 s**, tuzlamalı (`N=10`) süre **1.44 s** (fark yalnızca **-0.08 s**).
- **Bedeli / Sınırı:** Çarpıklık 1.03× gibi dengeli bir düzeydeyken tuzlama ikinci bir *shuffle* aşaması ekleyerek gereksiz karmaşıklık yaratır. Ancak ileride tek bir bölgenin veya tek bir sensörün trafiği 10×–50× artarsa `N=10` tuzlama devreye alınmalıdır.

### K-C05 · Servis Deposu Seçimi (MongoDB 7.0) ve İndeks Tasarımı (Z6) — `ÖLÇÜLDÜ`
- **Bağlam:** S3 üzerindeki Parquet dosyaları analitik toplu sorgular için hızlıdır ancak web panosu ve REST API'nin tekil sensör kartı, bölge sıralaması ve son anomali listesi gibi düşük gecikmeli (`p99 ≤ 2000 ms`) istekleri için uygun değildir.
- **Alternatifler:** (1) Doğrudan S3 Parquet üzerinden API sunmak · (2) İlişkisel veritabanı (PostgreSQL) · **(3) Belge tabanlı servis deposu (MongoDB 7.0)**.
- **Seçim:** (3) MongoDB 7.0 (`bsm461` veritabanı) seçildi (`servis/z6_mongo_yukle_olcum.py`, kanıt: `kanit/c_z6_servis_gecikme.txt`).
- **Gerekçe (Ölçüm):**
  - `sensor_ozet` (200 belge), `bolge_saat_ozet` (168 belge), `akis_a1_anomali` (2.000 belge) ve `akis_a2_susan` koleksiyonları S3'ten **1.53 s** içinde yüklendi.
  - Bileşik indeksler: `sensor_id` (unique), `(bolge, anomali_sayisi)`, `(bolge, saat)`, `(esik_asildi, pencere_bit)`.
  - **1.000 karışık okuma sorgusunda gecikme:** `p50 = 0.27 ms`, `p95 = 0.37 ms`, **`p99 = 0.46 ms`**, `max = 1.60 ms` (hedef `≤ 2000 ms` sınırının çok altında).
- **Bedeli:** S3 ile MongoDB arasında ek bir senkronizasyon adımı (`akis_senkronize_et`) gerekir; veri iki katmanda tutulduğu için nihai tutarlılık (eventual consistency) modeli kabul edilmiştir.

### K-C06 · Canlı Pano ve REST API Mimarisi (`http://localhost:8000`) — `ÖLÇÜLDÜ`
- **Bağlam:** Ekip arkadaşlarının (Rol A ve B) üreteceği toplu ve akış tablolarını tek bir ekranda birleştiren, sıfır ek imaj derleme maliyetiyle çalışan bir sunum arayüzü gerekiyordu.
- **Seçim:** `araclar` konteyneri üzerinde Python `ThreadingHTTPServer` tabanlı `servis/pano_api.py` uygulaması (`8000:8000`) geliştirildi (`kanit/c_pano.png`).
- **Özellikler:** `/api/saglik`, `/api/ozet`, `/api/sensor/{id}`, `/api/anomaliler` ve `/api/susan` uç noktaları; 7 bölgeye göre dinamik filtreleme; B5 aşamasında `s3://tablolar/akis_iot_a1` ve `akis_iot_a2` çıktıları oluştuğunda otomatik senkronizasyon.

---

## Soru Bankası (Üye C Sunum Provası)
1. **30 günlük veri hacmini ve bant genişliğini nasıl hesapladınız?** → `kanit/c_tani_olcek_kirli.txt` (143.54 B/olay × 2.000/s = 692.99 GB/30 gün; 3 kopya 2.08 TB; tepe 4.59 Mbps).
2. **Ham veride hangi kirli kayıtları buldunuz ve ne yaptınız?** → 6.030 tam tekrar silindi, 6.038 eksik alanlı kayıt `s3://islenmis/iot/karantina` altına ayrıldı; 2.987.932 temiz kayıt Parquet'e dönüştürüldü.
3. **JSONL yerine Parquet kullanmak ne kazandırdı?** → 410.66 MB → 77.14 MB (%81.2 disk tasarrufu) ve 1.75 s → 0.19 s (9.2× hızlı okuma).
4. **Neden tuzlama (salting) uygulamadınız?** → Sensör başı olay çarpıklığı yalnızca 1.03× olduğu için tuzlama ek shuffle maliyeti getiriyor (`1.52 s` vs `1.44 s`).
5. **Servis katmanında neden doğrudan S3 yerine MongoDB kullandınız ve p99 kaç çıktı?** → Tekil nokta sorguları ve sıralı listeler için indekslenmiş MongoDB koleksiyonları kullanıldı; 1.000 sorguda `p99 = 0.46 ms` ölçüldü (hedef `≤ 2000 ms`).
