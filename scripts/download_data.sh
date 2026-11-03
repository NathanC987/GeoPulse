#!/usr/bin/env bash
# Download the public LBSN datasets used by GeoPulse into data/raw/.
#   - Foursquare NYC + Tokyo check-ins (TSMC2014, Dingqi Yang et al.)
#   - SNAP Gowalla check-ins + friendship graph (Cho, Myers, Leskovec, KDD 2011)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RAW="$ROOT/data/raw"
mkdir -p "$RAW"
cd "$RAW"

fetch() {  # fetch <output> <url>...
  local out="$1"; shift
  if [[ -s "$out" ]]; then echo "✓ $out already present"; return 0; fi
  for url in "$@"; do
    echo "↓ $out  <-  $url"
    if curl -fL --retry 3 --connect-timeout 20 -o "$out.part" "$url"; then
      mv "$out.part" "$out"; return 0
    fi
  done
  rm -f "$out.part"
  return 1
}

fetch loc-gowalla_totalCheckins.txt.gz https://snap.stanford.edu/data/loc-gowalla_totalCheckins.txt.gz
fetch loc-gowalla_edges.txt.gz         https://snap.stanford.edu/data/loc-gowalla_edges.txt.gz

if ! fetch dataset_tsmc2014.zip \
      http://www-public.imtbs-tsp.eu/~zhang_da/pub/dataset_tsmc2014.zip \
      http://www-public.it-sudparis.eu/~zhang_da/pub/dataset_tsmc2014.zip; then
  cat <<'EOF'
!! Could not download the Foursquare TSMC2014 dataset automatically.
   Download it manually from Kaggle ("Foursquare NYC and Tokyo Check-ins",
   https://www.kaggle.com/datasets/chetanism/foursquare-nyc-and-tokyo-checkin-dataset)
   and place dataset_TSMC2014_NYC.txt and dataset_TSMC2014_TKY.txt in data/raw/.
EOF
  exit 1
fi

if [[ ! -s dataset_TSMC2014_NYC.txt ]]; then
  # The zip contains a dataset_tsmc2014/ folder; flatten it.
  python3 - <<'EOF'
import zipfile, os
with zipfile.ZipFile("dataset_tsmc2014.zip") as z:
    for name in z.namelist():
        base = os.path.basename(name)
        if base.startswith("dataset_TSMC2014") and base.endswith(".txt"):
            with z.open(name) as src, open(base, "wb") as dst:
                dst.write(src.read())
            print("  extracted", base)
EOF
fi

echo
ls -lh "$RAW"
