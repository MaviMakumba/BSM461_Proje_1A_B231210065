# C — Sunum Notları (60–90 sn Canlı Gösterim & Savunma)

## Canlı Gösterim Akışı (Üye C Bölümü)
1. **Veri Kalitesi ve Depolama Kazancı (15 sn):** `kanit/c_z10_temizleme_ozeti.txt` üzerinden 3M ham kayıttan 6.030 tekrar ve 6.038 eksik kaydın karantinaya ayrıldığını, Parquet dönüşümüyle **410.66 MB → 77.14 MB (%81.2 kazanç)** sağlandığını göster.
2. **Sıcak Nokta Analizi (15 sn):** `kanit/c_z3_sicak_nokta.txt` üzerinden sensör çarpıklığının **1.03×** çıktığını, bu nedenle tuzlamanın ek kazanç getirmediğini (`1.52 s` vs `1.44 s`) vurgula.
3. **MongoDB Performansı ve Canlı Pano (30 sn):** Tarayıcıda `http://localhost:8000` panosunu aç; `p99 = 0.46 ms` kartını, bölge filtresini, en çok anomali üreten `TX-200` sensörlerini (`sensor-050..059`) ve `son_gorulme` sütunlu susan sensör tablosunu göster.
