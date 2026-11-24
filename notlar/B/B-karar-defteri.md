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

### K-02 · S3 healthcheck `localhost` → `s3` (C'nin değişikliği) — `KARAR: korunur, README'ye yazılır` (24 Kas)
- **Belirti (C):** Orijinal hâlde `bsm461-s3` 2 dk boyunca `unhealthy` kaldı → `s3-kur`, `spark`, `araclar` başlamadı.
  Hata: `wget: can't connect to remote host: Connection refused`.
- **Kök neden (bizim makinede doğrulandı, 24 Kas):** SeaweedFS 9333 portunu **yalnızca IPv4**'te dinliyor
  (`127.0.0.1:9333` ve konteyner IP'si `172.18.0.3:9333`; `netstat -tln`). Konteynerin `/etc/hosts` dosyasında `localhost`
  hem `127.0.0.1` hem `::1` (IPv6) olarak tanımlı; busybox `wget` önce `::1`'i deniyor → orada dinleyen yok → reddedildi.
  Deneme: `wget localhost:9333` → çıkış 1 · `wget 127.0.0.1:9333` → 0 · `wget s3:9333` → 0.
  (C'nin "127.0.0.1'e bağlanmıyor" yorumu tam doğru değil: 127.0.0.1'de dinliyor, sorun IPv6 çözümlemesi.)
- **Alternatifler:** (1) `s3:9333` (C'nin çözümü; Docker iç DNS'i konteyner IP'sine çözer) · (2) `127.0.0.1:9333` (IPv4'e zorlar,
  servis adına bağlı değil) · (3) orijinal (bizim ve C'nin makinesinde çalışmıyor).
- **Seçim:** (1) korunur; çalışıyor ve ekip zaten kullanıyor. (2) de eşdeğer bir çözüm; README'de ikisi de anılacak.
- **Hoca sorarsa:** *"Kiti neden değiştirdiniz?"* → healthcheck IPv6 `localhost`'a gidiyordu, servis yalnızca IPv4'te dinliyor;
  kanıt yukarıdaki üç `wget` denemesi. Hocanın makinesinde (macOS) farklı davranması çözümleme sırasının farkından olabilir (**doğrulanmadı**).

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

**A'nın cevapları (24 Kas):**
1. *Anomali eşiği:* A sensöre özel eşik öneriyor: her sensörün geçmişinden normal aralık bir kez hesaplanır, aynı eşik tablosu hem A1'de
   hem T2'de kullanılır. → **Ortak eşik tablosu fikri iyi.** ⚠️ **Tuzak:** Eşik her sensörün *kendi* geçmişinden
   (ör. kendi p99'u ya da ort ± 3σ) çıkarsa her sensör kendi oynaklığına göre normalleşir: gürültülü bir sensörün aralığı
   geniş olur ve anomali oranları sensörler arasında yapay biçimde eşitlenir. T2'nin sorusu "hangi model ya da
   kurulum yılı daha çok anomali üretiyor?" olduğu için bu eşitleme T2'yi anlamsızlaştırabilir. Alternatifler: bölge bazlı aralık
   (iklim farkını kapsar, sensör farkını gizlemez) · sabit fiziksel sınırlar · sensör bazlı ama dirençli (medyan ± k·MAD).
   **B3'te A ile birlikte ve veriye bakarak karar verilecek.** Eşik tablosu da sözleşmeye eklenecek (öneri: `s3a://tablolar/iot_esik`, A yazar, B okur).
2. *Veri kaybı / susma:* A, `eksik / beklenen` oranını, sensörün aktif süresini beklenen gönderim aralığına bölerek hesaplamayı öneriyor ve
   "susma kaç ölçüm sonra?" diye soruyor; 2 dk'ya kadar geç geleni kayıp saymayalım diyor. → Önce **veriden sensör başına
   gönderim aralığını ve dağılımını ölçmeliyiz** (düzenli mi, rastgele mi?). ⚠️ Akışta aralık bizim seçtiğimiz `--hiz`'e bağlı
   (--hiz 20, 200 sensör → sensör başına ~10 s; hesap, ölçüm değil). Toplu veride bu aralık farklıdır (3M olay / 200 sensör / 14 gün
   → ~81 s; hesap). Bu yüzden kural "art arda N ölçüm" olarak değil, "beklenen aralığın k katı" olarak tanımlanmalı. Ayrıca toplu veride
   geç veri sorunu yoktur, çünkü bütün olaylar S3'te. 2 dk kuralı yalnızca akışta geçerli. **B4'te karar.**
3. *Toplu veri komutu:* `docker compose exec -T araclar python araclar/uretici.py --grup B231210065 --sablon iot --toplu 3000000 --gun 14`.
   A'da komut 130 kodu ile bitmiş (130 = kesilme sinyali, SIGINT), ama S3'te 14 gün, 14 dosya ve 3.000.000 kayıt sayılmış. → README'ye bu komut
   ve doğrulama adımı yazılacak.
4. *Z2 ölçümü:* A düzeltecek (aynı kayıtlar üzerinden karşılaştırma, satır sayıları, makine bilgisi). ✔

**C'nin cevabı — akış çıktı şemaları (24 Kas):**
- `akis_iot_a1`: ORTAK §6.3'teki 5 sütun yeterli (`pencere_bas, pencere_bit, anahtar, deger, esik_asildi`).
- `akis_iot_a2`: 5 sütun + **`son_gorulme TIMESTAMP`** (sensörün son veri gönderdiği an). Pano "ne zamandır susuyor"u da gösterecek.
- Statik bilgiyi (bölge, model) akışa ekleme: C, Mongo'ya yazarken `sensor_id` ile `sensorler.csv`'den zenginleştirir.

### K-04 · `startingOffsets` = **earliest** — `KARAR` (B1, 24 Kas)
- **Bağlam:** Akış işi ilk kez açıldığında (checkpoint yokken) Kafka defterinin neresinden okumaya başlayacak?
  Checkpoint varsa bu ayar **yok sayılır**; iş checkpoint'teki offset'ten devam eder.
- **Alternatifler:**
  | | earliest (baştan) | latest (şu andan) | zaman damgasıyla (`startingOffsetsByTimestamp`) |
  |---|---|---|---|
  | Artı | Hiçbir olay atlanmaz; iş üreticiden geç açılsa da sayım tam → deney ölçümü güvenilir | Canlı sistemin ilk kurulumunda doğal; eski olaylar için uyarı yağmaz | "Şu saatten itibaren" — ikisinin arası |
  | Eksi | Konuda eski mesaj varsa onları da işler → her deneyden önce konu + checkpoint temizlenmeli. Checkpoint kaybolursa yeniden işler → **çift sayım** | İş açılana kadar (~20–30 s) gelen olaylar hiç okunmaz → deneyde sahte "kayıp". Checkpoint kaybolursa aradaki veri **sessizce atlanır** | Karmaşık; deneylerimize katkısı yok |
- **Seçim (Eren):** **earliest** — *"deneylerde hiçbir olay kaçmasın diye."*
- **Uygulama kuralı:** Her deneyden önce konu silinip yeniden oluşturulur ve checkpoint klasörü silinir (README/deney betiklerine yazılacak).
- **İdeal durum (gerçek sistem):** İlk kurulumda `latest` (ya da zaman damgasıyla başlangıç) + **checkpoint kalıcı ve yedekli** (S3'te, çoğaltılmış).
  Sonraki her başlatma checkpoint'ten devam eder. Checkpoint kaybı bir olaydır (incident): `latest` kaybı gizler, `earliest` ise çift sayım üretir.
  Çift sayıma karşı sink idempotent olmalıdır (K-14). Yani bu ayar asıl güvenceyi sağlamaz, asıl güvence checkpoint'tir.
- **Hoca sorarsa:**
  - *"earliest/latest farkı?"* → defterin başı ↔ şu an; yalnızca ilk açılışta geçerli.
  - *"İş yeniden başlarsa nereden devam eder?"* → checkpoint'teki offset'ten; `startingOffsets` yok sayılır (B7'de gösterilecek).
  - *"latest geç gelen olayı kaçırır mı?"* → Hayır. `latest` *offset* (yazılma sırası) ile ilgilidir, *olay zamanı* ile değil.
    İş açıldıktan sonra Kafka'ya yazılan geç bir olay okunur.
  - *"Checkpoint silinirse?"* → latest: sessiz kayıp · earliest: çift sayım.

### K-05 · Kafka konusu `sensorler` = **3 bölüm** — `KARAR` (B2, 24 Kas)
- **Bağlam:** Bölüm sayısı, aynı anda kaç okuyucunun paralel çalışabileceğini belirler. Sonradan **azaltılamaz**;
  artırılırsa anahtar→bölüm eşlemesi değişir (geçişte anahtar başına sıra bozulur).
- **Ölçüm (B2 ölçümleri §2-3):** 20.000 olaylık yığın (tepe × 5 s) 1/3/6/12 bölümde ort. 617/507/516/508 ms → hepsi 5.000 ms'nin
  çok altında; 3'ten sonra kazanç yok. Denge (`sensor_id`): 3 bölüm 1,064 · 6 bölüm 1,399 · 12 bölüm 1,450.
- **Alternatifler:** 1 (basit, büyümeye kapalı) · **3** · 6 (büyüme payı, ama dengesiz ve bugün kazanç yok).
- **Seçim + gerekçe (Eren):** *"Ölçümde 1, 3, 6 ve 12 bölümün hepsi tepe yükü 5 saniyenin çok altında işledi; 3 bölüm en dengeli
  dağılımı verdi ve gelecekte üç makineye bölmeye izin veriyor."*
- **Bedeli:** Yük ~10 katına çıkarsa (ölçülen kapasite ~39.000 olay/s ≈ tepe × 10) tek makine sınıra gelir; o zaman bölüm artırımı
  + çok makine gerekir → C'nin Z1 10×/100× tartışmasına bağlanır.
- **Uygulama:** Konu, otomatik oluşmaya bırakılmadan `is/akis/konu_hazirla.py` ile açıkça 3 bölüm / replika 1 kurulur
  (yönerge Adım 11.1; K-04 gereği her deneyden önce de çalıştırılır).
- **Hoca sorarsa:**
  - *"Neden 3?"* → yukarıdaki cümle + tablo.
  - *"Yük 10 katına çıkarsa?"* → ~39.000/s ölçülen kapasite sınırı; bölüm artırılır, okuyucular birden çok makineye yayılır.
    Bölüm artırımı anahtar eşlemesini değiştirir, bu yüzden geçiş planlı yapılır.
  - *"Bölümü azaltabilir misiniz?"* → Hayır, Kafka izin vermez; yeni konu açıp taşımak gerekir.
  - *"Replika neden 1?"* → Kitte tek Kafka düğümü var. Üretimde RF=3, `min.insync.replicas=2` olmalı (Z0 garanti tablosu).

### K-06 · Kafka anahtarı = **`sensor_id`** — `KARAR` (B2, 24 Kas)
- **Bağlam:** Anahtar, mesajın hangi bölüme yazılacağını belirler (hash(anahtar) mod bölüm sayısı). Aynı anahtar → aynı bölüm → o
  anahtarın mesajları **yazılma sırasıyla** durur.
- **Ölçüm (3 bölüm, en dolu ÷ ortalama):** `sensor_id` 1,064 · anahtarsız 1,007 · `bolge` 1,700 (bir bölüm diğerinin ~4 katı).
- **Alternatifler:** **`sensor_id`** · anahtarsız (en dengeli; sensör başına sıra yok) · `bolge` (7 değer → sıcak bölüm).
- **Seçim + gerekçe (Eren):** *"Aynı sensörün ölçümleri aynı bölümde geliş sırasıyla kalsın diye sensor_id seçtim; 3 bölümde dağılım
  dengeli (1,06), bölge anahtarı ise bir bölümü 4 kat doldurdu."*
- **Dürüst sınır:** Spark sorgularımız sensöre göre gruplarken veriyi zaten yeniden dağıtıyor (shuffle) ve olay zamanına göre
  sıralıyor. Bu yüzden A1 ve A2'nin sonucu anahtar seçimine bağlı değil. Anahtarın getirisi şunlar: Kafka'yı doğrudan okuyan başka tüketiciler
  için sensör başına sıra, aynı sensörün tekrar kopyalarının aynı bölüme düşmesi ve ileride "her sensörün son durumu" için log
  compaction imkânı. Sıra **yazılma** sırasıdır, olay zamanı sırası değil (B1: 144 mesaj geri gitti).
- **Hoca sorarsa:**
  - *"Sıra nerede garanti?"* → Yalnızca aynı bölüm içinde, yazılma sırası olarak; konunun tamamında garanti yok.
  - *"Bölüm sayısını artırırsanız?"* → 6'da 1,40, 12'de 1,45: 200 anahtar az olduğu için denge bozuluyor.
  - *"Sıcak sensör olursa?"* → Bir sensör çok yoğun gönderirse bölümü ısınır; çözümü tuzlama (sensor_id + rastgele ek),
    bedeli sensör başına sıranın kaybı. (Z3 C'de; C'nin ölçümüyle bağlanacak.)

---

## Bekleyen kararlar (bölüm geldikçe yukarıya taşınır)

| Kod | Karar | Bölüm | Ölçüm planı (kısa) |
|---|---|---|---|
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

## B2 ölçümleri — konu tasarımı (24 Kas, kanıt: `kanit/b2/`, `kanit/olcum/B.csv`)

**1) Üretici tepeye çıkabiliyor mu?** `--hiz 4000` → 30 s'de 116.803 olay = **3.893 olay/s** (%97). `--hiz 2000` → 1.946/s.
Tekrar oranı ~%0,2 (Kafka − üretici: 238 / 117.041).

**2) Spark yığın süresi ↔ bölüm sayısı** (`is/olcum/b2_bolum_kapasite.py`; aynı 117.041 mesaj; yığın = 20.000 olay = tepe × 5 s;
iş yükü A1 benzeri: JSON → filtre → watermark 2 dk → 1 dk pencere × sensör; çıkış `noop`; 3 tekrar; ilk yığın hariç)

| Bölüm | Ort. yığın süresi | İşleme hızı | Tepe (4.000/s) payı |
|---|---|---|---|
| 1 | 617 ms | 32.049 olay/s | ~8× fazlası |
| 3 | 507 ms | 39.192 olay/s | ~10× |
| 6 | 516 ms | 38.878 olay/s | ~10× |
| 12 | 508 ms | 39.188 olay/s | ~10× |
En kötü yığın **1.070 ms** (5.000 ms sınırının çok altında). Soğuk başlangıçta ilk yığın 2,8–3,4 s.
**Yorum:** Bu makinede bölüm sayısı darboğaz değil; 3'ten sonra kazanç yok (yığın başına sabit maliyet baskın).
**Sınırlılık:** Tek makine, Spark `local[*]`; çıkış `noop` ve checkpoint yerel → Delta + S3 checkpoint maliyeti B5'te ayrıca ölçülecek.

**3) Anahtar ↔ bölüm dengesi** (en dolu ÷ ortalama; aynı 117.041 mesaj)

| Anahtar | 3 bölüm | 6 bölüm | 12 bölüm |
|---|---|---|---|
| `sensor_id` (200 anahtar) | 1,064 (üreticinin kendi bölümleyicisiyle 1,105) | 1,399 | 1,450 |
| `bolge` (7 anahtar) | **1,700** (17.147 / 33.553 / 66.341) | – | – |
| anahtarsız | **1,007** | – | – |
Not: Aynı anahtar, üretici (Python/librdkafka) ve Spark (Java istemci) ile farklı bölüme düşebiliyor (1,105 ↔ 1,064).
Muhtemel neden: iki istemcinin varsayılan hash yöntemi farklı (**doğrulanmadı**). Pratik sonucu şu: aynı konuya iki farklı
istemci yazarsa anahtar başına sıra garantisi bozulabilir.
Yan bulgu: 53 mesajda `bolge` eksik (%0,045) → C'nin kirli veri sayımına.

**4) acks / idempotent** (`--hiz 100000 --sure 10`, 3 tekrar, kanıt: `kanit/b2/acks-olcum.txt`, `kanit/b2/acks-gunluk/`)

| Ayar | Ort. hız (duvar süresiyle) | Tekrar (Kafka − üretici) |
|---|---|---|
| acks=all + idempotent açık | 48.623 olay/s (2 başarılı tekrar) | ~%0,2 |
| acks=all + idempotent kapalı | 48.272 | ~%0,2 |
| acks=1 | 47.839 | ~%0,2 |
| acks=0 | 48.355 | ~%0,2 |
**Yorum:** Fark %1,6 içinde, yani anlamlı değil. Nedenleri: (1) Kitte tek Kafka düğümü ve replika 1 olduğu için `all` ile `1` aynı işi yapar (beklenecek başka kopya yok).
(2) Darboğaz Python üreticisinin kendisi (olay üretimi, CPU), Kafka'nın onay süresi değil. (3) Ağ hatası olmadığı için yeniden deneme
olmadı → idempotentliğin düzelteceği bir şey yoktu. Tekrarlar (%0,2) idempotent açıkken de var, çünkü uygulama düzeyindeler (B1 bulgu 3).
**Ek bulgu (Z11):** Tepenin ~25 katında (100.000/s) 24 çalıştırmanın 4'ünde üretici `BufferError: Local: Queue full` ile çöktü;
yerel kuyruktaki **100.000 mesaj kayboldu** (ör. v2r3: Kafka'da yalnızca 118.378). Kafka anlık yetişemeyince üreticinin
arabelleği doluyor ve uygulama bunu yakalamıyor → kayıp. acks ayarı bunu önlemez (mesajlar hiç gönderilmemişti).
**Sınırlılık:** Duvar süresi `docker exec` ve Python başlangıcını içeriyor; hız mutlak değil, ayarlar arası karşılaştırma için.

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
