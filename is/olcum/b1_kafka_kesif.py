"""B1 — Kafka keşfi: konsol tüketicisi dökümünden veri desenlerini SAYAR (üretici koduna bakmadan).

Döküm şu komutla alınır (bkz. notlar/B/B-karar-defteri.md, B1):
  docker exec bsm461-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic sensorler --from-beginning --timeout-ms 8000 \
    --property print.partition=true --property print.key=true \
    --property print.timestamp=true --property print.offset=true > dokum.txt
Kullanım: python is/olcum/b1_kafka_kesif.py dokum.txt
"""
import json
import sys
from collections import Counter
from datetime import datetime, timezone

satirlar = []
for ham in open(sys.argv[1], encoding="utf-8"):
    ham = ham.rstrip("\n")
    if not ham.startswith("CreateTime:"):
        continue
    ts, bolum, ofset, anahtar, deger = ham.split("\t", 4)
    kafka_ms = int(ts.split(":")[1])
    try:
        olay = json.loads(deger)
    except json.JSONDecodeError:
        olay = None
    satirlar.append(dict(bolum=int(bolum.split(":")[1]), ofset=int(ofset.split(":")[1]),
                         anahtar=anahtar, kafka_ms=kafka_ms, ham=deger, olay=olay))

n = len(satirlar)
print(f"Toplam mesaj: {n}")

# 1) Bölüm dağılımı (anahtar = sensor_id)
bd = Counter(s["bolum"] for s in satirlar)
ort = n / len(bd)
print("Bölüm dağılımı:", dict(sorted(bd.items())), f"· en dolu ÷ ortalama = {max(bd.values()) / ort:.3f}")
print("Farklı anahtar (sensör) sayısı:", len({s["anahtar"] for s in satirlar}))

# 2) Alanlar ve kirli kayıtlar
bozuk_json = [s for s in satirlar if s["olay"] is None]
alanlar = Counter(k for s in satirlar if s["olay"] for k in s["olay"])
print("Alanlar (kaç mesajda var):", dict(alanlar))
eksik = Counter()
for s in satirlar:
    if not s["olay"]:
        continue
    for k in alanlar:
        if k not in s["olay"] or s["olay"][k] in (None, ""):
            eksik[k] += 1
print("Bozuk JSON:", len(bozuk_json), "· eksik/boş alan:", dict(eksik))
for s in satirlar:
    if s["olay"] and any(k not in s["olay"] or s["olay"][k] in (None, "") for k in alanlar):
        print(f"  örnek kirli: bölüm {s['bolum']} ofset {s['ofset']} Kafka anahtarı={s['anahtar']!r} → {s['ham']}")
tipler = Counter((k, type(v).__name__) for s in satirlar if s["olay"] for k, v in s["olay"].items())
print("Alan tipleri:", dict(tipler))

# 3) Gecikme = Kafka'ya yazılma zamanı − olay_zamani
def ms(t):
    return int(datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp() * 1000)

gecikmeler = []
for s in satirlar:
    try:
        gecikmeler.append((s["kafka_ms"] - ms(s["olay"]["olay_zamani"])) / 1000)
    except Exception:
        pass
gecikmeler.sort()
esik = 5.0
gec = [g for g in gecikmeler if g > esik]
print(f"Gecikme ölçülen: {len(gecikmeler)} · >{esik:.0f} s geç: {len(gec)} (%{100 * len(gec) / len(gecikmeler):.2f})"
      f" · en büyük gecikme: {gecikmeler[-1]:.1f} s · medyan: {gecikmeler[len(gecikmeler) // 2]:.3f} s")
kovalar = Counter(min(int(g // 30) * 30, 150) for g in gec)
print("Geç olayların dağılımı (30 s kovalar, alt sınır s):", dict(sorted(kovalar.items())))

# 4) Tekrarlar
ayni_ham = Counter(s["ham"] for s in satirlar)
tekrar = {k: v for k, v in ayni_ham.items() if v > 1}
print(f"Birebir aynı mesaj (tekrar) grubu: {len(tekrar)} · fazladan kopya: {sum(v - 1 for v in tekrar.values())}")
for k in list(tekrar)[:3]:
    kopya = [(s["bolum"], s["ofset"], s["kafka_ms"]) for s in satirlar if s["ham"] == k]
    print("  örnek tekrar:", k, "→ (bölüm, ofset, kafka_ms):", kopya)

anahtar2 = Counter((s["olay"].get("sensor_id"), s["olay"]["olay_zamani"]) for s in satirlar if s["olay"])
cakisma = {k: v for k, v in anahtar2.items() if v > 1}
farkli_degerli = [k for k in cakisma
                  if len({s["ham"] for s in satirlar if s["olay"] and
                          (s["olay"].get("sensor_id"), s["olay"]["olay_zamani"]) == k}) > 1]
print(f"(sensor_id, olay_zamani) aynı olan grup: {len(cakisma)} · bunlardan değerleri FARKLI olan: {len(farkli_degerli)}")
for k in farkli_degerli[:2]:
    print("  örnek:", [s["ham"] for s in satirlar if s["olay"] and (s["olay"].get("sensor_id"), s["olay"]["olay_zamani"]) == k])

# 5) Bölüm içi sıra: aynı sensörün olay_zamani Kafka sırasında geri gidiyor mu?
son = {}
geri = 0
for s in sorted(satirlar, key=lambda x: (x["bolum"], x["ofset"])):
    if not s["olay"]:
        continue
    t = ms(s["olay"]["olay_zamani"])
    if s["anahtar"] in son and t < son[s["anahtar"]]:
        geri += 1
    son[s["anahtar"]] = max(t, son.get(s["anahtar"], t))
print("Aynı sensörde olay_zamani'nın Kafka sırasında geri gittiği mesaj:", geri)
