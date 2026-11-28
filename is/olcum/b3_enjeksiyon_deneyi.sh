#!/usr/bin/env bash
# B3 / K-09d — A1 arıza enjeksiyonu deneyi: tespit süresi ve yanlış alarm.
# Kullanım (Git Bash, repo kökünde):  bash is/olcum/b3_enjeksiyon_deneyi.sh <etiket> [konumlar=5,300,570,595] [atak_sure=30]
# Sıra: konu sıfırlanır (K-04) → A1 (oran, K-09b/c, trigger 2 s) açılır → hazır olunca normal üretici --hiz 2000 başlar
#       → en az 2 dk ısınmadan sonraki ilk pencere sınırından itibaren her konum ayrı pencerede enjekte edilir
#       → son ataktan 60 s sonra her şey durur. Uyarılar kanit/b3/enjeksiyon-uyarilar.csv, ataklar kanit/b3/enjeksiyon-ataklar.csv
# Not: Doğal arıza saatine denk gelmemeli (o gün üreticinin kendi atağı da 050–059'da olur) → analizde atak dışı uyarı sayılır.
set -u
cd "$(dirname "$0")/../.."
ET=$1; KONUM=${2:-5,300,570,595}; ASURE=${3:-30}
HZ="kanit/b3/.hazir-$ET"
rm -f "$HZ"
export MSYS_NO_PATHCONV=1
docker compose exec -T araclar python is/akis/konu_hazirla.py > /dev/null
docker compose exec -T spark spark-submit is/akis/a1_anomali.py --mod oran --trigger 2 --etiket "$ET" \
  --uyari-kayit kanit/b3/enjeksiyon-uyarilar.csv --sure 3600 --hazir-dosyasi "$HZ" > "kanit/b3/a1-$ET.log" 2>&1 &
SP=$!
until [ -f "$HZ" ]; do sleep 1; done
SIMDI=$(date +%s)
ILK=$(( (SIMDI + 120 + 599) / 600 * 600 ))
N=$(echo "$KONUM" | tr ',' '\n' | wc -l); SONK=$(echo "$KONUM" | tr ',' '\n' | tail -1)
BITIS=$(( ILK + (N - 1) * 600 + SONK + ${ASURE%.*} + 60 ))
echo "[$ET] iş hazır · ilk pencere $(date -u -d @$ILK +%H:%M) UTC · bitiş $(date -u -d @$BITIS +%H:%M:%S) UTC"
docker compose exec -T araclar python araclar/uretici.py --grup B231210065 --sablon iot --hiz 2000 --sure $(( BITIS - SIMDI )) \
  > "kanit/b3/uretici-$ET.log" 2>&1 &
UP=$!
docker compose exec -T araclar python -X utf8 is/olcum/b3_ariza_enjeksiyonu.py --ilk-pencere "$ILK" --konumlar "$KONUM" \
  --atak-sure "$ASURE" --etiket "$ET" 2>&1 | tee "kanit/b3/enjeksiyon-$ET.log"
wait "$UP"
# A1'i durdur (--sure 3600 üst sınır; deney daha kısa): sürücü sürecini konteynerde sonlandır
docker compose exec -T spark bash -c "pkill -f 'a1_anomali.py.*--etiket $ET' || true"
wait "$SP" 2>/dev/null
rm -f "$HZ"
echo "[$ET] bitti · $(grep -o 'toplam [0-9.]*' "kanit/b3/uretici-$ET.log") · uyarı satırı: $(grep -c "^$ET," kanit/b3/enjeksiyon-uyarilar.csv 2>/dev/null || echo 0)"
