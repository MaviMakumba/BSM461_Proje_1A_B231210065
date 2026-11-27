# -*- coding: utf-8 -*-
"""Üye C · IoT Servis Katmanı (REST API) ve Canlı Web Panosu (Port 8000)"""
import json
import sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, "/kit/araclar")
from pymongo import MongoClient, DESCENDING, ASCENDING
from ortak import MONGO_ADRES, s3

client = MongoClient(MONGO_ADRES)
db = client["bsm461"]


def akis_senkronize_et():
    """Ekip arkadaşının B5'te üreteceği S3 akis_iot_a1 / akis_iot_a2 çıktıları varsa MongoDB'ye aktarır."""
    try:
        import duckdb
        from ortak import S3_ADRES, S3_ANAHTAR, S3_GIZLI
        istemci = s3()
        for tablo_adi, kol_adi in [("akis_iot_a1", "akis_a1_anomali"), ("akis_iot_a2", "akis_a2_susan")]:
            nesneler = istemci.list_objects_v2(Bucket="tablolar", Prefix=f"{tablo_adi}/").get("Contents", [])
            if any(o["Key"].endswith(".parquet") for o in nesneler):
                con = duckdb.connect()
                con.execute(f"LOAD httpfs; SET s3_endpoint='{S3_ADRES.split('//')[1]}'; SET s3_use_ssl=false; "
                            f"SET s3_url_style='path'; SET s3_access_key_id='{S3_ANAHTAR}'; "
                            f"SET s3_secret_access_key='{S3_GIZLI}'; SET s3_region='us-east-1';")
                kayitlar = con.execute(f"SELECT * FROM read_parquet('s3://tablolar/{tablo_adi}/*.parquet') LIMIT 500").fetchdf().to_dict(orient="records")
                if kayitlar:
                    db[kol_adi].delete_many({})
                    db[kol_adi].insert_many(kayitlar)
    except Exception:
        pass


HTML_PANO = """<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<title>BSM 461 · IoT Telemetri & Anomali Panosu (Üye C)</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 24px; }
  h1 { margin: 0 0 6px 0; font-size: 22px; color: #38bdf8; }
  .alt { color: #94a3b8; font-size: 13px; margin-bottom: 20px; }
  .kartlar { display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; margin-bottom: 22px; }
  .kart { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 16px; }
  .kart .etiket { font-size: 12px; color: #94a3b8; text-transform: uppercase; }
  .kart .deger { font-size: 26px; font-weight: 700; margin-top: 6px; color: #f8fafc; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
  .panel { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 16px; }
  h2 { font-size: 15px; margin: 0 0 12px 0; color: #f1f5f9; border-bottom: 1px solid #334155; padding-bottom: 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 7px 8px; border-bottom: 1px solid #334155; }
  th { color: #94a3b8; font-weight: 600; }
  .rozet-kirmizi { background: #7f1d1d; color: #fecaca; padding: 2px 8px; border-radius: 99px; font-size: 11px; }
  .rozet-yesil { background: #14532d; color: #bbf7d0; padding: 2px 8px; border-radius: 99px; font-size: 11px; }
  select, button { background: #0f172a; color: #e2e8f0; border: 1px solid #475569; padding: 6px 10px; border-radius: 6px; font-size: 13px; }
  button { cursor: pointer; background: #0284c7; border-color: #0284c7; font-weight: 600; }
</style>
</head>
<body>
  <div style="display:flex; justify-content:space-between; align-items:center;">
    <div>
      <h1>BSM 461 · IoT Sensör Telemetrisi ve Anomali Panosu</h1>
      <div class="alt">Servis Deposu: MongoDB 7.0 (bsm461) · Nesne Deposu: SeaweedFS S3 (Parquet) · Üye C</div>
    </div>
    <div>
      <select id="bolgeSec" onchange="yukle()">
        <option value="">Tüm Bölgeler (7 Bölge)</option>
        <option>Marmara</option><option>Karadeniz</option><option>Ege</option>
        <option>Akdeniz</option><option>İç Anadolu</option><option>Doğu Anadolu</option><option>Güneydoğu</option>
      </select>
      <button onclick="yukle()">Yenile</button>
    </div>
  </div>

  <div class="kartlar">
    <div class="kart"><div class="etiket">Toplam Aktif Sensör</div><div class="deger" id="k-sensor">-</div></div>
    <div class="kart"><div class="etiket">Temiz İşlenen Ölçüm</div><div class="deger" id="k-olcum">-</div></div>
    <div class="kart"><div class="etiket">Tespit Edilen Anomali</div><div class="deger" style="color:#f87171" id="k-anomali">-</div></div>
    <div class="kart"><div class="etiket">API p99 Hedefi</div><div class="deger" style="color:#4ade80">0.46 ms</div></div>
  </div>

  <div class="grid">
    <div class="panel">
      <h2>En Çok Anomali Üreten Sensörler (Toplu + Boyut Zenginleştirme)</h2>
      <table>
        <thead><tr><th>Sensör</th><th>Bölge</th><th>Model</th><th>Ort. Sıcaklık</th><th>Anomali</th></tr></thead>
        <tbody id="tb-sensor"></tbody>
      </table>
    </div>
    <div class="panel">
      <h2>Son Eşik Aşımı Uyarıları (A1 Akış / Anomali Kayıtları)</h2>
      <table>
        <thead><tr><th>Sensör</th><th>Bölge</th><th>Değer</th><th>Zaman</th><th>Durum</th></tr></thead>
        <tbody id="tb-anomali"></tbody>
      </table>
    </div>
  </div>

  <div class="panel" style="margin-top:18px;">
    <h2>Susan Sensör İzleme Tablosu (A2 Akış · son_gorulme)</h2>
    <table>
      <thead><tr><th>Sensör</th><th>Bölge</th><th>Model</th><th>Son Görülme (son_gorulme)</th><th>Durum</th></tr></thead>
      <tbody id="tb-susan"></tbody>
    </table>
  </div>

<script>
async function yukle() {
  const bolge = document.getElementById("bolgeSec").value;
  const q = bolge ? "?bolge=" + encodeURIComponent(bolge) : "";
  const ozet = await (await fetch("/api/ozet" + q)).json();
  document.getElementById("k-sensor").textContent = ozet.toplam_sensor;
  document.getElementById("k-olcum").textContent = ozet.toplam_olcum.toLocaleString("tr-TR");
  document.getElementById("k-anomali").textContent = ozet.toplam_anomali.toLocaleString("tr-TR");

  document.getElementById("tb-sensor").innerHTML = ozet.en_riskli_sensorler.map(s =>
    `<tr><td><b>${s.sensor_id}</b></td><td>${s.bolge}</td><td>${s.model}</td><td>${s.ort_sicaklik} °C</td><td><span class="rozet-kirmizi">${s.anomali_sayisi}</span></td></tr>`
  ).join("");

  const anom = await (await fetch("/api/anomaliler" + q)).json();
  document.getElementById("tb-anomali").innerHTML = anom.slice(0, 10).map(a =>
    `<tr><td><b>${a.anahtar}</b></td><td>${a.bolge || "-"}</td><td>${a.deger}</td><td>${(a.pencere_bit || "").slice(0,19)}</td><td><span class="rozet-kirmizi">EŞİK AŞILDI</span></td></tr>`
  ).join("");

  const susan = await (await fetch("/api/susan" + q)).json();
  document.getElementById("tb-susan").innerHTML = susan.slice(0, 10).map(s =>
    `<tr><td><b>${s.anahtar}</b></td><td>${s.bolge || "-"}</td><td>${s.model || "-"}</td><td>${s.son_gorulme || "-"}</td><td><span class="${s.esik_asildi ? 'rozet-kirmizi' : 'rozet-yesil'}">${s.esik_asildi ? 'SUSKUN' : 'AKTİF'}</span></td></tr>`
  ).join("");
}
yukle();
setInterval(yukle, 10000);
</script>
</body>
</html>
"""


class PanoHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def json_gonder(self, veri, kod=200):
        govde = json.dumps(veri, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(kod)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(govde)))
        self.end_headers()
        self.wfile.write(govde)

    def do_GET(self):
        ayrisik = urlparse(self.path)
        yol = ayrisik.path
        qs = parse_qs(ayrisik.query)
        bolge = qs.get("bolge", [None])[0]

        if yol == "/" or yol == "/index.html":
            govde = HTML_PANO.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(govde)))
            self.end_headers()
            self.wfile.write(govde)
        elif yol == "/api/saglik":
            self.json_gonder({"durum": "saglikli", "mongo": "bsm461", "sensor_sayisi": db.sensor_ozet.count_documents({})})
        elif yol == "/api/ozet":
            filtre = {"bolge": bolge} if bolge else {}
            sensorler = list(db.sensor_ozet.find(filtre, {"_id": 0}).sort("anomali_sayisi", DESCENDING))
            self.json_gonder({
                "toplam_sensor": len(sensorler),
                "toplam_olcum": sum(s["toplam_olcum"] for s in sensorler),
                "toplam_anomali": sum(s["anomali_sayisi"] for s in sensorler),
                "en_riskli_sensorler": sensorler[:10],
            })
        elif yol.startswith("/api/sensor/"):
            sid = yol.split("/api/sensor/")[1]
            kayit = db.sensor_ozet.find_one({"sensor_id": sid}, {"_id": 0})
            self.json_gonder(kayit or {"hata": "Sensör bulunamadı"}, 200 if kayit else 404)
        elif yol == "/api/anomaliler":
            akis_senkronize_et()
            filtre = {"esik_asildi": True}
            if bolge:
                filtre["bolge"] = bolge
            kayitlar = list(db.akis_a1_anomali.find(filtre, {"_id": 0}).sort("pencere_bit", DESCENDING).limit(50))
            self.json_gonder(kayitlar)
        elif yol == "/api/susan":
            akis_senkronize_et()
            filtre = {"bolge": bolge} if bolge else {}
            kayitlar = list(db.akis_a2_susan.find(filtre, {"_id": 0}).sort("son_gorulme", ASCENDING).limit(50))
            self.json_gonder(kayitlar)
        else:
            self.json_gonder({"hata": "Bilinmeyen uç nokta"}, 404)


if __name__ == "__main__":
    sunucu = ThreadingHTTPServer(("0.0.0.0", 8000), PanoHandler)
    print("Pano ve REST API hazir -> http://localhost:8000")
    sunucu.serve_forever()
