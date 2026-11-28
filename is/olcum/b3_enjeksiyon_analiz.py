# -*- coding: utf-8 -*-
"""B3 / K-09d — Enjeksiyon deneyinin analizi: tespit süresi (atak başı → ⚠) ve yanlış alarm.

Çalıştırma: docker compose exec araclar python is/olcum/b3_enjeksiyon_analiz.py <etiket> [<etiket> …]
Girdi : kanit/b3/enjeksiyon-ataklar.csv (atak_bas = ilk anormal mesajların Kafka'ya yazıldığı an)
        kanit/b3/enjeksiyon-uyarilar.csv (t_bitti = A1'in ⚠ satırını ürettiği an; aynı makinenin saati)
Çıktı : ekrana + kanit/b3/enjeksiyon-sonuc-<etiket>.txt
Tanımlar:
  tespit süresi = sensörün atak başından sonraki İLK ⚠ anı − atak_bas  (atak bitiminden 60 s sonrasına kadar bakılır)
  yanlış alarm  = (a) saldırılmayan bir sensör için ⚠, (b) saldırılan sensör için hiçbir atak aralığına düşmeyen ⚠
"""
import csv
import statistics as st
import sys

etiketler = sys.argv[1:]
ataklar = [r for r in csv.DictReader(open("/kit/kanit/b3/enjeksiyon-ataklar.csv", encoding="utf-8")) if r["etiket"] in etiketler]
uyarilar = [r for r in csv.DictReader(open("/kit/kanit/b3/enjeksiyon-uyarilar.csv", encoding="utf-8")) if r["etiket"] in etiketler]
for u in uyarilar:
    u["t"] = float(u["t_bitti"])

satirlar, sonuc = [], {}
aralik = []                                   # (etiket, sensör, başlangıç, bitiş+60)
for at in ataklar:
    b, s = (int(x) for x in at["sensorler"].split("-"))
    sensorler = [f"sensor-{i:03d}" for i in range(b, s + 1)]
    bas, bit = float(at["atak_bas"]), float(at["atak_bit"])
    for sid in sensorler:
        aralik.append((at["etiket"], sid, float(at["ilk_gonderim"]) - 1, bit + 60))
        ilk = min((u for u in uyarilar if u["etiket"] == at["etiket"] and u["anahtar"] == sid and bas - 1 <= u["t"] <= bit + 60),
                  key=lambda u: u["t"], default=None)
        sure = round(ilk["t"] - bas, 2) if ilk else None
        hangi = ilk["pencere_bas"][11:19] if ilk else "-"     # uyarıyı veren pencerenin başı (K-09e: kayan pencere)
        satirlar.append((at["etiket"], int(at["konum_s"]), sid, sure, hangi,
                         f'{ilk["anomali_sayisi"]}/{ilk["olcum_sayisi"]}' if ilk else "-"))
        sonuc.setdefault(int(at["konum_s"]), []).append(sure)

saldirilan = {(e, sid) for e, sid, _, _ in aralik}
yanlis_a = [u for u in uyarilar if (u["etiket"], u["anahtar"]) not in saldirilan]
yanlis_b = [u for u in uyarilar if (u["etiket"], u["anahtar"]) in saldirilan
            and not any(e == u["etiket"] and sid == u["anahtar"] and a0 <= u["t"] <= a1 for e, sid, a0, a1 in aralik)]

cikti = [f"# K-09d enjeksiyon sonucu · etiketler {', '.join(etiketler)} · A1 oran kuralı (etiketin kuralı için karar defterine bak: enj1 = 10 dk sabit/≥12, enj2 = 1 dk kayan/≥15; trigger 2 s)",
         "", "## Konuma göre tespit süresi (s) — konum = atağın 10 dk'lık pencere içinde başladığı saniye",
         "konum_s | tespit_edilen | en_az | medyan | en_cok | 5 s içinde"]
for k in sorted(sonuc):
    v = [x for x in sonuc[k] if x is not None]
    cikti.append(f"{k} | {len(v)}/{len(sonuc[k])} | " + (f"{min(v)} | {st.median(v)} | {max(v)} | {sum(x <= 5 for x in v)}/{len(sonuc[k])}" if v else "- | - | - | 0"))
hepsi = [x for v in sonuc.values() for x in v if x is not None]
if hepsi:
    cikti.append(f"TÜMÜ | {len(hepsi)}/{sum(len(v) for v in sonuc.values())} | {min(hepsi)} | {st.median(hepsi)} | {max(hepsi)} | "
                 f"{sum(x <= 5 for x in hepsi)}/{sum(len(v) for v in sonuc.values())}")
cikti += ["", f"## Yanlış alarm: saldırılmayan sensörde ⚠ = {len(yanlis_a)} · saldırılan sensörde atak dışı ⚠ = {len(yanlis_b)}"]
for u in yanlis_a + yanlis_b:
    cikti.append(f"  {u['etiket']} {u['anahtar']} pencere {u['pencere_bas']} · {u['anomali_sayisi']}/{u['olcum_sayisi']}")
cikti += ["", "## Sensör başına", "etiket | konum_s | sensor | tespit_s | uyari_penceresi_bas | ilk ⚠ anında anomali/ölçüm"]
cikti += [" | ".join(str(x) for x in s) for s in satirlar]
metin = "\n".join(cikti)
print(metin)
open(f"/kit/kanit/b3/enjeksiyon-sonuc-{'-'.join(etiketler)}.txt", "w", encoding="utf-8").write(metin + "\n")
