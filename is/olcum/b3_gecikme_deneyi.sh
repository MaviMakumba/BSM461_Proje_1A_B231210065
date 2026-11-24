#!/usr/bin/env bash
# B3 — A1 uyarı gecikmesi deneyi (K-08 trigger süresi, K-09b anlık ↔ pencere).
# Kullanım (Git Bash / WSL, repo kökünde):  bash is/olcum/b3_gecikme_deneyi.sh <anlik|pencere|ikisi> <trigger_sn> <uretim_sn> <etiket>
# Sıra: konu sıfırlanır (K-04) → Spark işi açılır → iş "veri bekliyor" olunca üretici başlar (--hiz 2000 = kart ortalaması)
#       → üretim biter → iş 15 s sonra durur. Gecikmeler kanit/b3/gecikme.csv'ye eklenir; günlükler kanit/b3/*.log
set -u
cd "$(dirname "$0")/../.."
MOD=$1; TRG=$2; SURE=$3; ET=$4
HZ="kanit/b3/.hazir-$ET"
rm -f "$HZ"
docker compose exec -T araclar python is/akis/konu_hazirla.py > /dev/null
docker compose exec -T spark spark-submit is/akis/a1_anomali.py --mod "$MOD" --trigger "$TRG" \
  --olcum kanit/b3/gecikme.csv --etiket "$ET" --sure $((SURE + 15)) --hazir-dosyasi "$HZ" > "kanit/b3/a1-$ET.log" 2>&1 &
SP=$!
until [ -f "$HZ" ]; do sleep 1; done
echo "[$ET] iş hazır, üretim başlıyor ($SURE s)"
docker compose exec -T araclar python araclar/uretici.py --grup B231210065 --sablon iot --hiz 2000 --sure "$SURE" \
  > "kanit/b3/uretici-$ET.log" 2>&1
wait "$SP"
rm -f "$HZ"
echo "[$ET] bitti · $(grep -o 'toplam [0-9.]*' "kanit/b3/uretici-$ET.log")"
