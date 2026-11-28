# Sunum Notları — B (Eren): Akış Hattı · BSM 461 · Grup B231210065 · Şablon IoT

> **Bu dosya ne için?** Sunumda kendi kısmımı kendim anlatacağım. Bu dosya NotebookLM gibi bir araca kaynak olarak
> verilecek; bu yüzden her bölüm tek başına anlaşılır yazıldı. Her konu: **basit anlatım → teknik ayrıntı →
> sayı/kanıt → hocanın sorabileceği sorular ve cevapları.** Sayılar yalnızca ölçtüklerimizdir (kaynak: `kanit/olcum/B.csv`).
> Karar gerekçelerinin tam hâli: `B-karar-defteri.md`.

**Sunumdaki yerim:** 3,5–5. dakika → akış tarafının en önemli kararı (watermark), ölçümüyle.
5–7. dakika → canlı gösterim: akış hattı veri işliyor (aksarsa ekran kaydı). Son 2 dakikadaki sorular: her karar sorulabilir.

---

## 0. Projenin bizim parçası tek cümlede

Sensörlerden saniyede 2.000 ölçüm geliyor. Biz bu ölçümleri Kafka'da sıraya koyuyor, Spark ile canlı işliyor ve
iki soruya **5 saniye içinde** cevap üretiyoruz: *Hangi sensör normal dışına çıktı?* ve *Hangi sensör sustu?*

```
üretici.py ──▶ Kafka (konu: sensorler) ──▶ Spark akış işi ──▶ Delta tablo (S3) ──▶ C'nin panosu / Mongo
sahte sensör   "defter": mesajları        her birkaç sn'de     akis_iot_a1
ölçümü üretir  numaralı sırayla tutar      okur, hesaplar        akis_iot_a2
```

## 1. Senaryo kartı ve ana çelişki (sunumun kalbi)

| Hız | Tepe | Uyarı süresi | Geç veri | Kayıp sınırı |
|---|---|---|---|---|
| 2.000 ölçüm/s | ×2 = 4.000/s | **5 s** | olayların **%15**'i **2 dk**'ya kadar geç | sayılmayan olay **≤ %0,1** |

**Basit anlatım:** Az olay kaçırmak için geç gelenleri 2 dakika beklemem gerekiyor. Ama uyarıyı 5 saniyede vermem
gerekiyor. İkisi aynı anda olamaz. Hangisinden nasıl ödün verdiğimizi **deneyle** gösteriyoruz.
**Çözümün özeti:** [B3/B6'da doldurulacak]

## 2. Temel kavramlar (kendi cümlelerimle)

- **Kafka bir defterdir, kuyruk değil.** Her mesaj sıra numarasıyla (**offset**) yazılır. Okunan mesaj silinmez,
  defterde kalır. Her okuyucu kendi **ayracını** tutar.
- **Konu (topic):** Defterin adı. Bizimki `sensorler`.
- **Bölüm (partition):** Defter, paralel okunabilsin diye birkaç ciltten oluşur. **Sıra yalnızca aynı cilt içinde korunur.**
- **Anahtar (key):** Mesajın hangi cilde yazılacağını belirler. Anahtarımız `sensor_id` → aynı sensör hep aynı ciltte.
- **Geç veri:** Ölçüm 10:00'da alındı ama 10:02'de geldi. Olay zamanı ile geliş zamanı farklı.
- **Watermark:** Spark'a "en fazla şu kadar geç gelen veriyi bekle, daha geç geleni at" demek.
  Uzun watermark → az kayıp ama geç sonuç. Kısa watermark → hızlı sonuç ama daha çok kayıp.
- **Checkpoint:** Spark'ın "nerede kaldım" defteri. İş çökerse oradan devam eder.
- **Delta:** S3'teki tablo biçimi. Yazma ya tamamen olur ya hiç olmaz (yarım tablo kalmaz).

## 3. Veriyi tanıdık (B1) — ne bulduk?

**Basit anlatım:** Üreticiyi 1 dakika çalıştırıp Kafka'ya düşen 1.171 mesajın hepsini bir betikle saydık.
Desenleri kodu okuyarak değil, **veriyi sayarak** bulduk.

| Bulgu | Sayı (ölçülen) | Neden önemli |
|---|---|---|
| Mesaj alanları | `olay_zamani, sensor_id, bolge, sicaklik, nem, titresim` · **olay kimliği yok** | Tekrarı ayıklamak zorlaşıyor |
| Geç olay | **%15,2** (178/1.171), en geç **119,8 s** | Kartla uyumlu; watermark'ı bu belirler |
| Tekrar | Üretici 1.168 dedi, Kafka'da 1.171 → **3 birebir kopya** | Sayımı şişirir; Spark'ta ayıklanmalı |
| Aynı sensör + aynı milisaniye, farklı değer | **38 grup** | "sensör + zaman" ile ayıklarsam gerçek veriyi silerim |
| Kafka sırasında olay zamanı geri giden | **144 mesaj** | Kafka yazılma sırasını korur, olay zamanı sırasını değil |
| `sensor_id` eksik | **1 mesaj** | Kirli kayıt, filtrelenmeli |

**Soru-cevap:**
- *S: Idempotent üretici açıkken neden hâlâ tekrar var?* → C: Idempotent ayar yalnızca **ağ hatasında yeniden
  denemenin** çift yazmasını önler. Burada uygulama aynı olayı iki ayrı kez gönderiyor; Kafka bunları iki farklı mesaj görür.
  Kanıt: kopyalar aynı bölümde, ardışık iki ayrı offset'te duruyor. Bu yüzden ayıklamayı Spark tarafında yapıyoruz.
- *S: Kafka sırayı garanti ediyor mu?* → C: Yalnızca aynı bölüm içinde ve yalnızca **yazılma** sırasını.
  Olay zamanı sırasını değil; 144 mesajda geri gittiğini ölçtük. Olay zamanı düzenini Spark'ta watermark ile kuruyoruz.

## 4. Karar: Spark Kafka'yı nereden okumaya başlar? → `earliest` (K-04)

**Basit anlatım:** Spark işi ilk açıldığında defterin başından mı okusun, yoksa yalnızca yeni mesajları mı? Ben
**baştan (earliest)** seçtim, çünkü deneylerde hiçbir olay kaçmasın istedim.

**Örnekle:** Üreticiyi 10:00:00'da başlattım, Spark işi ancak 10:00:25'te açılabildi. `latest` seçseydim ilk 25 saniyenin
olaylarını hiç görmeyecektim ve deneyde bunları yanlışlıkla "watermark kaybı" sanacaktım. `earliest` ile hepsini okuyorum.
Bedeli: defterde eski deneylerden kalan mesaj varsa onları da okur. Bu yüzden her deneyden önce konuyu ve checkpoint'i temizliyorum.

**Gerçek sistemde ideal olan:** İlk kurulumda `latest`, çünkü geçmiş 7 günün olayları için uyarı yağdırmak anlamsız.
Sonraki her başlatmada iş **checkpoint**'ten devam eder. Asıl güvence bu ayar değil, checkpoint'tir.

**Soru-cevap:**
- *S: İş yeniden başlarsa nereden devam eder?* → C: Checkpoint'teki offset'ten. Checkpoint varken bu ayar hiç kullanılmaz.
- *S: Checkpoint silinirse?* → C: `latest` ile aradaki veri sessizce atlanır (kayıp). `earliest` ile eski veri yeniden işlenir (çift sayım).
- *S: `latest` geç gelen olayı kaçırır mı?* → C: Hayır. `latest` deftere *yazılma* sırasıyla ilgili, olay zamanıyla değil.

## 5. Konu tasarımı (B2) — bölüm sayısı, anahtar, acks

**Benzetme (postane):** Sensörler mektup gönderiyor. Kafka postane; mektupları **kutulara** atıyor. **Bölüm = kutu.**
Spark'ın okuyucuları memurlar; bir kutuya aynı anda bir memur bakabilir. **Anahtar = "mektup hangi kutuya?" kuralı.**

### 5.1 Kaç bölüm? → 3 (K-05)
**Kendi cümlem:** "Ölçümde 1, 3, 6 ve 12 bölümün hepsi tepe yükü 5 saniyenin çok altında işledi; 3 bölüm en dengeli dağılımı verdi
ve gelecekte üç makineye bölmeye izin veriyor."

**Nasıl ölçtüm:** Tepe yük 4.000 olay/s, uyarı 5 s → her 5 saniyede 20.000 olay gelir. Aynı 117.041 olayı 1, 3, 6 ve 12 bölümlü
konulara koydum. Spark'ın 20.000'lik her yığını kaç ms'de işlediğini 3'er kez ölçtüm.

| Bölüm | 20.000 olayı işleme süresi | Saniyede | Tepenin kaç katı |
|---|---|---|---|
| 1 | 617 ms | 32.049 | ~8 |
| 3 | 507 ms | 39.192 | ~10 |
| 6 | 516 ms | 38.878 | ~10 |
| 12 | 508 ms | 39.188 | ~10 |

**Ne çıktı:** Hız her seçenekte yetiyor; 3'ten sonra artmıyor. O yüzden karar hızla değil, **dengeyle** ve **büyüme payıyla** verildi.
Bölüm sayısı sonradan azaltılamıyor.

**Soru-cevap:**
- *S: Yük 10 katına çıkarsa?* → C: Bu makinede ölçülen sınır ~39.000/s, yani tepenin ~10 katı. Ötesi için bölüm artırılır ve
  okuma birden çok makineye yayılır. Bölüm artırımı sensörlerin hangi bölüme gittiğini değiştirdiği için planlı yapılır.
- *S: Bölüm sayısını azaltabilir misiniz?* → C: Hayır, Kafka izin vermez; yeni konu açıp taşımak gerekir.
- *S: Replika neden 1?* → C: Kitte tek Kafka sunucusu var. Gerçek sistemde 3 kopya ve "en az 2 kopya yazmadan onay verme" (`min.insync.replicas=2`).
- *S: Bu ölçümün sınırı ne?* → C: Tek bilgisayar, sonuç hiçbir yere yazılmadı. S3'e Delta yazma maliyetini B5'te ayrıca ölçtüm. [B5 sonucu eklenecek]

### 5.2 Anahtar? → `sensor_id` (K-06)
**Kendi cümlem:** "Aynı sensörün ölçümleri aynı bölümde geliş sırasıyla kalsın diye sensor_id seçtim; 3 bölümde dağılım dengeli (1,06),
bölge anahtarı ise bir bölümü 4 kat doldurdu."

| Anahtar | En dolu bölüm ÷ ortalama (3 bölüm) | Yorum |
|---|---|---|
| `sensor_id` (200 sensör) | 1,06 | dengeli + sensör başına sıra |
| `bolge` (7 bölge) | 1,70 | bir bölüm diğerinin ~4 katı → sıcak bölüm |
| anahtarsız | 1,01 | en dengeli ama sensörün mesajları dağılır, sıra yok |
Ayrıca `sensor_id` ile 6 bölümde 1,40, 12 bölümde 1,45 → 200 anahtar az; bölüm çoğaldıkça denge bozuluyor (5.1'in de gerekçesi).

**Sıra neden önemli (örnek):** sensor-012 önce "95 °C" (alarm), sonra "20 °C" (düzeldi) gönderdi. Aynı bölümde sırayla dururlar,
okuyan doğru anlar. Farklı bölümlerde "20" önce okunabilir ve "alarm sürüyor" diye yanlış sonuç çıkar.

**Soru-cevap:**
- *S: Sıra nerede garanti?* → C: Yalnızca aynı bölüm içinde ve yazılma sırası olarak. Konunun tamamında garanti yok.
- *S: Spark sorgunuz bu sıraya mı güveniyor?* → C: Hayır. Spark sensöre göre gruplarken veriyi yeniden dağıtıyor ve ölçüm saatine
  (olay zamanı) göre çalışıyor. Bu yüzden iki sorumuzun sonucu anahtardan etkilenmiyor. Anahtarın getirisi Kafka'yı doğrudan okuyan
  başka programlar ve tekrar kopyalarının aynı bölüme düşmesi.
- *S: Tek bir sensör çok yoğun gönderirse?* → C: O bölüm ısınır. Çözümü anahtara rastgele bir ek koymak (tuzlama); bedeli o sensörün sırasının kaybı.

### 5.3 Üretici onayı: acks=all + idempotent (K-07)
**Benzetme:** Gerçek Kafka'da aynı mektubun kopyasını tutan birkaç şube var. `acks` = "mektubu bıraktıktan sonra ne zaman 'tamam' diyeyim?"
- `acks=0`: kapıya atıp gidiyorum, makbuz yok → kaybolsa haberim olmaz.
- `acks=1`: ana şube makbuz veriyor, kopyalar henüz yok → ana şube o an yanarsa mektup gider.
- `acks=all`: makbuzu ancak bütün şubeler kopyayı alınca veriyorlar → en güvenli, biraz bekletir.
- **Idempotent:** Makbuz yolda kaybolup mektubu tekrar atarsam postane sıra numarasından aynı mektup olduğunu anlar, ikincisini almaz.

**Gerekçe:** "Kartımızda kayıp sınırı %0,1; gerçek sistemde bir Kafka sunucusu çökerse yalnızca bütün kopyaları yazılmış mesajı onaylatmak
kaybı önler. Kitte tek sunucu olduğu için bu güvencenin bedeli ölçülemedi: dört ayar arasında fark %1,6'nın altında kaldı."

| Ayar | Saniyede gönderilen (3 tekrar) | Çift mesaj |
|---|---|---|
| all + idempotent | 48.623 | %0,2 |
| all, idempotent kapalı | 48.272 | %0,2 |
| 1 | 47.839 | %0,2 |
| 0 | 48.355 | %0,2 |

**Neden fark yok:** (1) Kitte tek şube (tek Kafka sunucusu) → "hepsini bekle" = "ana şubeyi bekle". (2) Yavaş olan mektubu yazan
(Python üreticisi), postane değil. (3) Ağ sorunu olmadı → tekrar gönderim yok → idempotentin düzelteceği bir şey yok.

**Beklenmedik bulgu (kırılma):** Tepenin ~25 katında (100.000/s) 24 çalıştırmanın 4'ünde üretici `Queue full` ile çöktü ve elindeki
100.000 mesaj kayboldu. Postane bir an yavaşladı, çanta taştı. Bu kaybı acks önleyemez, çünkü mesajlar postaneye hiç ulaşmamıştı.

**Soru-cevap:**
- *S: Ölçümde fark yoksa faydasını nereden biliyorsun?* → C: Kırılma deneyinde Kafka'yı yazma sırasında durdurup acks=0 ile acks=all'da
  kaybı saydım. [B8 sonucu eklenecek]
- *S: acks=all kaybı tamamen önler mi?* → C: Hayır. Henüz gönderilmemiş, üreticinin kuyruğunda bekleyen mesajları korumaz (Queue full bulgusu).
- *S: Bedeli ne?* → C: Gerçek sistemde her mesajda en yavaş kopyayı beklemek, yani gecikme. Bir kopya düşerse yazma reddedilir:
  kullanılabilirlik yerine tutarlılığı seçmiş oluyoruz.

## 6. A1 — normal dışı sensör (B3)

### 6.1 "Normal dışı" ne demek? (K-09a, A ile ortak)
**Kural:** Bir ölçüm, kendi bölgesinin **o saatteki** ortanca (medyan) değerinden, normal oynama payının **5 katından** fazla saparsa anomalidir.
Eşikler 3 milyonluk geçmiş veriden bir kez hesaplandı (7 bölge × 24 saat × 3 ölçü). Akış ve toplu taraf aynı tabloyu kullanıyor.

**Kendi cümlem:** "Normal değer bölgeye ve günün saatine göre değişiyor; bu tabanla 5 katlık eşikte alarm oranı k arttıkça sabit kaldı (%0,40),
yani yalnızca gerçek sıçramaları yakalıyor. 3σ kuralı ise nemde hiç sıçrama yokken saniyede yaklaşık 5 sahte alarm üretiyordu."

**Neden bölge ve saat?** Doğu Anadolu'nun ortalaması 11 °C, Akdeniz'inki 29 °C. Bölge içinde gece-gündüz farkı ~8 °C. Saat de hesaba
katılınca normal oynama payı ±4 °C'den ±1 °C'ye iniyor, yani eşik 4 kat daha keskinleşiyor.

**Plato testi (en güçlü kanıtım):** Alarmın hassasiyetini azaltıyorum (k=4→5→6). Alarm sayısı düşmeye devam ediyorsa yakaladığım şey gürültü,
sabit kalıyorsa gerçek olay. Gerçek bir yangın, alarm az hassas olsa da alarmı çaldırır.

| | k=4 | k=5 | k=6 |
|---|---|---|---|
| Titreşim | %0,402 | %0,399 | %0,399 → sabit, gerçek |
| Sıcaklık (bölge + saat) | %0,400 | %0,399 | %0,399 → sabit, gerçek |
| Sıcaklık (yalnız bölge) | %0,373 | %0,324 | %0,259 → düşüyor, günlük döngü karışıyor |
| Nem | %0,005 | 0 | 0 → gerçek sıçrama yok |

**Neden 3σ değil?** Normal dağılımda değerlerin ~%0,27'si zaten 3σ dışındadır. Nemde tam bu oranı buldum (%0,25), yani bunlar sahte alarm.
Saniyede 2.000 ölçümde bu, saniyede ~5 sahte alarm eder (hesap). Her tostta çalan yangın alarmını kimse ciddiye almaz.

**Neden ortalama değil medyan?** Dev sıçramalar ortalamayı ve oynama payını şişiriyor. Titreşimde ortalamayla hesaplanan oynama 0,17,
gerçek oynama 0,05. Sıçramalar gerçek oynamanın ortalama 50 katı.

**Yan bulgu (A'nın T2'si için):** TX-200 modeli diğer modellerden ~4 kat fazla anomali üretiyor (%1,12'ye karşı %0,30).

**Soru-cevap:**
- *S: Bu kural neyi kaçırır?* → C: 5 kattan küçük ama sürekli kayan (drift eden) bir sensörü. Bu bir sıçrama değil eğilimdir ve ayrı bir yöntem ister.
- *S: Sensör başına eşik neden değil?* → C: Ölçtüm: aynı bölgedeki sensörler neredeyse aynı davranıyor; sensör ve bölge tabanı aynı oranı verdi
  (%0,672 ↔ %0,675). Bölge daha basit (504 satıra karşı 600) ve açıklaması kolay.
- *S: Akıştaki anomaliyle toplu tablodaki aynı mı?* → C: Evet, ikisi aynı eşik tablosunu okuyor.

### 6.2 Spark kaç saniyede bir bakar? → 2 saniye (K-08)
**Kavram:** Spark akışı tek tek değil, küçük paketler (mikro-yığın) hâlinde işler. "Trigger" paketlerin kaç saniyede bir alınacağıdır.
Benzetme: Postacı kutuyu her 2 saniyede bir boşaltıyor. Bir mektup en kötü ihtimalle 2 saniye kutuda bekler, üstüne ayıklama süresi eklenir.

**Ölçtüğüm:** En kötü uyarı gecikmesi: trigger 1 s → 1,77 s · **2 s → 2,43 s** · 5 s → 5,65 s (yığınların çoğu 5 s'yi aştı).

**Kendi cümlem:** "Ölçümde en kötü uyarı gecikmesi trigger 2 sn'de 2,4 sn, 5 sn'de 5,65 sn çıktı; 5 sn trigger kartın sınırını yığınların
çoğunda aşıyor. 1 sn daha hızlı ama iki kat fazla küçük dosya üretiyor; 2 sn sınırın yarısında kalarak ikisini dengeliyor."

**Soru-cevap:**
- *S: Kart 5 s diyor, neden 5 değil?* → C: Gecikme = bekleme + işleme. 5 s bekleyip üstüne yarım saniye işlersem sınırı aşarım; ölçtüm, aştı.
- *S: Bedeli?* → C: Her 2 saniyede bir yazma = çok sayıda küçük dosya. Delta'nın sıkıştırma (OPTIMIZE) özelliğiyle çözülür, B5'te ölçeceğim.

### 6.3 "Normal dışı sensör" = tek sıçrama değil, sıçrama ORANI (K-09b)
**Sorun:** Her sıçramaya uyarı verince saniyede ~12 uyarı çıktı; 200 sensörün 197'si "arızalı" göründü.
**Bulgu:** Veride her sensör ara sıra tek bir saçma değer üretiyor (%0,2–0,4). Bu ölçüm hatası. Gerçekten arızalı 10 sensör (hepsi TX-200)
ise %2,0–2,7 oranında ve art arda sıçrıyor. Arada boşluk var: %0,4 ile %2,0 arasında hiç sensör yok.
Benzetme: Hastane monitörü kablo oynadığında bir kez öter; hemşire tek ötüşe koşmaz, ötüşler sıklaşınca koşar.

**Kural:** Sensör, 10 dakikalık pencerede sıçrama oranı **%1'i** geçerse normal dışıdır. %1 iki grubun tam arasında.
Pencerenin bitmesini beklemiyorum; satır her 2 saniyede güncelleniyor (update kipi), eşik geçildiği anda uyarı çıkıyor.

**Kendi cümlem:** "Veride her sensör ara sıra tekil sıçrama yapıyor (%0,2–0,4), oysa arızalı 10 sensör %2,0–2,7 oranında ve kümeli sıçrıyor.
Bu yüzden 'normal dışı sensör'ü tek bir ölçümle değil, 10 dakikalık penceredeki sıçrama oranıyla (%1 eşiği) tanımladım ve satırı her
2 saniyede güncelleyerek 5 saniye sınırını koruyorum."

- *S: Pencere 10 dk, 5 s'yi nasıl tutuyorsun?* → C: Hız ve kesinlik iki ayrı an. Uyarı eşik geçildiği yığında (~2 s) çıkıyor; kesin sayı pencere
  kapanıp geç veri süresi (watermark) geçince oluşuyor.

### 6.4 "En az 12 sıçrama" şartı (K-09c)
**Sorun:** Pencere yeni başladığında 50 ölçümde 1 sıçrama bile %2 eder → sağlam sensöre yanlış alarm.
**İki çare denedim (20 dk akış, 600 sensör-pencere):**
- "En az 1000 ölçüm olsun": yanlış alarm 0, ama her pencere başında 100 saniye kör.
- "En az m sıçrama olsun": sağlam sensörler tesadüfen en fazla **8** sıçramaya ulaştı; m=10'da yanlış alarm 0. Pay bırakıp **12** seçtim.

**Kendi cümlem:** "Sağlam sensörler 20 dakikalık akışta tesadüfen en fazla 8 sıçramaya ulaştı; 12 eşiği 600 sensör-pencerede sıfır yanlış
alarm verirken, yarı yarıya sıçrayan bir arıza atağını birkaç saniyede yakalaması bekleniyor. Ölçüm sayısına dayalı eşik ise her pencere
başında 100 saniye kör kalıyordu." [tespit süresi enjeksiyon deneyinden sonra eklenecek]

- *S: Zayıf noktası?* → C: Atak pencerenin sonlarına doğru başlarsa, pencerede biriken normal ölçümler oranı aşağı çeker ve %1'i geçmek
  birkaç saniye uzar (hesapla en kötü ~8 s). Bunu enjeksiyon deneyinde ölçüyorum. [sonuç eklenecek]

### 6.5 Arızayı akışta nasıl gösterdim? → arıza enjeksiyonu (K-09d)
**Bulgu:** Geçmiş veride arıza sürekli değil: **günde bir kez, 10 sensör aynı anda, 1–2 saatlik atak**; atak sırasında ölçümlerin yarısı sıçrama.
20 dakikalık akış deneyime bu atak denk gelmedi.
**Çözüm:** Normal akış sürerken bu 10 sensör için, geçmiş veride ölçtüğüm atak davranışını (oran %50, gerçek sapma büyüklükleri) Kafka'ya
basıyorum ve uyarının kaç saniyede çıktığını ölçüyorum. Benzetme: Yangın tatbikatı. Gerçek yangını beklemiyorsun; duman makinesiyle
dedektörün kaç saniyede öttüğünü ölçüyorsun.

**Kendi cümlem:** "Arıza atakları günde bir kez rastgele bir saatte geldiği için akışta beklemek pratik değil; toplu veride ölçtüğüm atak
davranışını (oran %50, gerçek sapma büyüklükleri) akışa enjekte ederek tespit süresini doğrudan ölçeceğim." [deney bitince "ölçtüm" yapılıp sonuç eklenecek]

- *S: Veriyi uydurmuş olmuyor musun?* → C: Atağın oranı ve büyüklüğü gerçek veriden ölçüldü; yalnızca ne zaman başlayacağını ben seçtim.
  Raporda da "enjeksiyon" diye açıkça yazıyorum. Aynı aracı canlı gösterimde kullanacağım.

## 7. A2 — susan sensör (B4)
[doldurulacak]

## 8. Watermark deneyi (B6) — sunumdaki ana kararım
[doldurulacak: iki oranda kaçırılan olay sayısı, seçilen watermark ve gerekçesi]

## 9. Yeniden başlatma ve teslim garantisi (B7)
[doldurulacak]

## 10. Sistem nerede kırılır? (B8)
[doldurulacak]

## 11. Canlı gösterim senaryosu (5–7. dk)
[B9'da yazılacak: hangi komut, ekranda ne görünecek, ne söylenecek, aksarsa yedek]

## 12. Diğer rollerden bilmem gerekenler (sorular bana da gelebilir)
- **A (toplu/depolama):** T1 = sıcaklık, nem ve titreşimin gün içinde bölgelere göre değişimi · T2 = anomali ve veri kaybı
  oranının sensör modeli ve kurulum yılına göre karşılaştırması. Ham JSON 430,6 MB, Parquet 45,2 MB
  (A satır sayılarını eşitleyip yeniden ölçecek). [A'nın kararları geldikçe eklenecek]
- **Ortak:** "Anomali" tanımı akışta (A1) ve toplu tarafta (T2) aynı eşik tablosundan geliyor → iki taraf tutarlı.
- **C (temizleme, servis, pano):** Z10: tekrarları ve eksik/hatalı kayıtları ayırıp temiz veriyi tarih bölümlü Parquet'e yazdı
  (JSON → Parquet %81 küçülme). Z6: özetleri MongoDB'ye koydu, 1.000 sorguda p99 0,46 ms. Pano `http://localhost:8000`, benim akış
  tablolarımı (`akis_iot_a1/_a2`) okuyup gösteriyor. (C'nin sayıları K-15 sonrası yeniden üretilecek.)

## 13. Ekipte sayılar neden tutmuyordu? (K-15) — "aynı tarif, farklı gün"
**Basit anlatım:** Kitin veri üreticisi, verinin 14 gününü "bugünden geriye" sayıyor. Ben veriyi 24 Kasım'da ürettim, C başka bir gün.
Aynı grup kodunu kullandık ama farklı veri çıktı: bende 6.050 tekrar kayıt, C'de 6.030.

**Benzetme:** Aynı sırada dizilmiş bir zar dizisi düşün (grup kodu bunu belirliyor). Her olay bu diziden sırayla zar çekiyor. Arıza
saati takvim tarihinden hesaplanıyor; arıza saatine düşen bir olay fazladan zar çekiyor. Tarih değişince arıza başka saate düşüyor,
fazladan zarlar başka yerde çekiliyor ve o andan sonra herkesin eline başka zar geliyor. Ayrıca bütün zaman damgaları bir gün kayıyor.

**Teknik ayrıntı:**
- `uretici.py` toplu üretim: `bitis = datetime.now()` (UTC gece yarısı), günler = bitiş − 14 … bitiş − 1.
- Arıza saati = `crc32("YYYY-AA-GG" + tuz) % 24` → tarihe bağlı.
- Önce yöntem farkından şüphelendim. Ölçtüm: bizim veride Spark `dropDuplicates`, DuckDB `DISTINCT` hepsi 6.050 veriyor. Yani fark veride.
- Çözüm: üreticiye `--son-gun 2026-11-23` seçeneği ekledim (varsayılan davranış değişmedi).

**Ölçülen sayı:** Eski 24 Kas verisini yedekledim, yeni seçenekle yeniden ürettim, her günün satırlarını sıralayıp sha256 aldım:
**14 günün 14'ü birebir aynı.** Artık herkes (hoca dahil) README'deki tek komutla aynı 3.000.000 kaydı alıyor.

**Hoca sorarsa:**
- *"Kiti neden değiştirdiniz?"* → Varsayılanı değiştirmedim, sadece tarih sabitleme seçeneği ekledim; README'de "Kitten farklarımız" tablosunda.
- *"Ben şimdi çalıştırsam sizin sayıları alır mıyım?"* → `--son-gun 2026-11-23` verirseniz evet, birebir.

## 14. Kendi kodumda bulduğum hata (eşik tablosu) — "hata bulup ölçmek"
**Basit anlatım:** Eşik tablosunu hesaplarken tekrar kayıtları silmek istedim ama silme işlemini yanlış sütunlarla yaptım. Gerçekte farklı
olan ama bölgesi, saati ve değerleri tesadüfen aynı olan 4.696 ölçümü de silmişim.

**Örnek:** Marmara'da saat 14'te iki farklı sensör aynı anda 25,14 °C, %47,9 nem, 0,071 titreşim ölçmüş olabilir. Bunlar iki gerçek ölçüm.
Ben sadece (bölge, saat, değerler) sütunlarına bakarak "aynı" dediğim için birini attım. Doğrusu: önce bütün satıra bakıp tekrarı silmek
(sensör ve zaman dahil), sonra sütun seçmek.

**Ölçülen etki:** 168 bölge×saat hücresinin birkaçında eşik çok az değişti (medyan en çok 0,01 °C; dağılım en çok %3).
Sıcaklık anomalisi sayısı **11.926 → 11.926, hiç değişmedi.** Düzelttim, tablo yeniden yazıldı.

**Hoca sorarsa:** *"Hiç hata yaptınız mı?"* → Evet; bulduk, etkisini ölçtük, defterde duruyor (K-09a düzeltme notu).

## 15. Pano benim tablolarımı nasıl okumalı? (Delta okuma) — K-11 hazırlığı
**Basit anlatım:** Delta tablosu bir klasör + bir "içindekiler" defteri (`_delta_log`). Bir satırı güncellediğimde eski dosya silinmiyor,
deftere "bu dosya artık geçersiz" diye not düşülüyor. Klasördeki bütün `.parquet` dosyalarını doğrudan okuyan, eski satırları da görür.

**Ölçülen:** Küçük bir tabloya iki kez üzerine yazdım. Spark ve DuckDB `delta_scan` **100** satır görüyor (doğru). Klasördeki
`*.parquet` dosyalarını düz okuyan **200** satır görüyor (eski sürüm dahil). A1 her yığında MERGE ile güncellendiği için (K-09b) bu
fark bizim tabloda kesin ortaya çıkar → C'nin panosu `delta_scan` kullanmalı.

**Hoca sorarsa:** *"Panonuz neden eski uyarı göstermiyor?"* → Delta'yı defteriyle birlikte okuyoruz; düz Parquet okumanın neden yanlış
olduğunu ölçtük (200 ↔ 100).
- **C (servis/pano/kalite):** [eklenecek]
