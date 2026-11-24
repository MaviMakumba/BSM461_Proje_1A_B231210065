# -*- coding: utf-8 -*-
"""B3 — K-09b oran kuralının çevrimdışı analizi: "en az N ölçüm" şartı için en iyi N hangisi?

Girdi: a1_anomali.py --mod oran --ham-kayit ile her yığında yazılan pencere satırları (sensör × pencere × yığın)
       + kanit/b3/arizali-sensorler.txt (toplu veride oranı > %1 olan sensörler = "doğru cevap")
Her N için: sensör, bir pencerede ilk kez (ölçüm ≥ N ve oran > %1) olduğu yığında işaretlenir.
  doğru  = arızalı listede olan ve işaretlenen · yanlış alarm = listede olmayan ama işaretlenen
  kaçan  = listede olan, penceresinde ≥ N ölçüm biriktiği hâlde işaretlenmeyen
  tespit süresi = sensörün o penceredeki ilk satırından işaretlendiği yığına kadar geçen süre
  sistem gecikmesi = satırdaki en son olayın Kafka'ya girişinden yığının bitişine
Kullanım: python is/olcum/b3_oran_analiz.py kanit/b3/oran-t2-ham.csv
"""
import csv
import statistics as st
import sys
from collections import defaultdict

ESIK = 1.0
arizali = {l.split(",")[0] for l in open("kanit/b3/arizali-sensorler.txt", encoding="utf-8") if l.startswith("sensor")}
satirlar = list(csv.DictReader(open(sys.argv[1], encoding="utf-8")))
for r in satirlar:
    r["yigin"] = int(r["yigin"]); r["t_bitti"] = float(r["t_bitti"]); r["son_kafka"] = float(r["son_kafka"])
    r["olcum_sayisi"] = int(r["olcum_sayisi"]); r["anomali_sayisi"] = int(r["anomali_sayisi"])
satirlar.sort(key=lambda r: r["yigin"])

seri = defaultdict(list)                     # (pencere, sensör) → yığın sırasıyla satırlar
for r in satirlar:
    seri[(r["pencere_bas"], r["anahtar"])].append(r)

pencereler = sorted({p for p, _ in seri})
print(f"Ham satır: {len(satirlar)} · pencere: {len(pencereler)} → {pencereler}")
print(f"Doğru cevap (toplu veriden, oran > %{ESIK}): {sorted(arizali)}")

print("\n## Pencere sonundaki oranlar (akışta da boşluk var mı?)")
for p in pencereler:
    son = {s: v[-1] for (pp, s), v in seri.items() if pp == p}
    n_ort = st.mean(v["olcum_sayisi"] for v in son.values())
    def oran(v): return 100 * v["anomali_sayisi"] / v["olcum_sayisi"]
    sag = [oran(v) for s, v in son.items() if s not in arizali]
    ari = [oran(v) for s, v in son.items() if s in arizali]
    print(f"  {p}: sensör {len(son)} · ort. ölçüm/sensör {n_ort:.0f} · sağlam oran {min(sag):.2f}–{max(sag):.2f}% · "
          f"arızalı oran {min(ari):.2f}–{max(ari):.2f}%" if ari else f"  {p}: arızalı yok")

print("\n## N'e göre sonuç (her pencere ayrı değerlendirilir)")
print("N | doğru | yanlış alarm | kaçan | tespit süresi (s) medyan / maks | sistem gecikmesi (s) medyan / maks")
for N in [100, 200, 300, 500, 750, 1000, 1500, 2000, 3000]:
    dogru = yanlis = kacan = 0
    sure, gecikme = [], []
    for (p, s), v in seri.items():
        ilk = v[0]["t_bitti"]
        isaret = next((r for r in v if r["olcum_sayisi"] >= N and 100 * r["anomali_sayisi"] / r["olcum_sayisi"] > ESIK), None)
        if isaret:
            if s in arizali:
                dogru += 1; sure.append(isaret["t_bitti"] - ilk)
            else:
                yanlis += 1
            gecikme.append(isaret["t_bitti"] - isaret["son_kafka"])
        elif s in arizali and v[-1]["olcum_sayisi"] >= N:
            kacan += 1
    f = lambda x: f"{st.median(x):.1f} / {max(x):.1f}" if x else "-"
    print(f"{N} | {dogru} | {yanlis} | {kacan} | {f(sure)} | {f(gecikme)}")

print("\n## Alternatif kural: oran > %1 VE en az m sıçrama (az ölçümde değil, yeterli sıçramada karar)")
print("m | yanlış alarm (sağlam sensör-pencere) | toplam sağlam sensör-pencere | en erken yanlış alarmın ölçüm sayısı")
saglam = [(k, v) for k, v in seri.items() if k[1] not in arizali]
for m in [3, 5, 8, 10, 12, 15, 20]:
    yanlis = [next((r for r in v if r["anomali_sayisi"] >= m and 100 * r["anomali_sayisi"] / r["olcum_sayisi"] > ESIK), None)
              for _, v in saglam]
    yanlis = [r for r in yanlis if r]
    print(f"{m} | {len(yanlis)} | {len(saglam)} | {min((r['olcum_sayisi'] for r in yanlis), default='-')}")

print("\n## Sağlam sensörlerde pencere içinde oran > %1 iken ulaşılan EN BÜYÜK sıçrama sayısı (dağılım)")
enb = []
for _, v in saglam:
    x = [r["anomali_sayisi"] for r in v if 100 * r["anomali_sayisi"] / r["olcum_sayisi"] > ESIK]
    enb.append(max(x) if x else 0)
from collections import Counter
print(sorted(Counter(enb).items()))
