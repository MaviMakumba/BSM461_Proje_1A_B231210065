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

### 5.3 acks / idempotent (K-07)
[ölçüm sürüyor — eklenecek]

## 6. A1 — normal dışı sensör (B3)
[doldurulacak]

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
- **C (servis/pano/kalite):** [eklenecek]
