INSTALL_DIR="${PWD}"
set -o pipefail

if [[ ! -d "${INSTALL_DIR}/ssb-dbgen" ]]; then
  echo "Downloading SSB DBGen..."
  git submodule update --init --recursive
  git pull
  cd ssb-dbgen
  cmake .
  cmake --build .
  cd ..
fi

echo "Generating SSB data..."
cd ssb-dbgen
./dbgen -v -s 100
cd ..
mkdir -p data/ssb
mv ssb-dbgen/*.tbl data/ssb

# NOTE: make sure the duckdb is installed!!!
sed -i"" -e "s|PATHVAR|${INSTALL_DIR}/data/ssb|" ./load_ssb.sql
sed -i"" -e "s|PATHVAR|${INSTALL_DIR}/data/ssb|" ./load_ssb_skew.sql

echo "Loading SSB data... [DuckDB]"
cat load_ssb.sql | duckdb ./ssb.duckdb
echo "Loading SSB-skew data... [DuckDB]"
cat load_ssb_skew.sql | duckdb ./ssb_skew.duckdb
mv data/ duckdb_data/

echo "Preparing benchmark data... [Postgres]"
mkdir -p data/ssb
mkdir -p data/ssb-skew

sed -i"" -e "s|PATHVAR|${INSTALL_DIR}|" ./export-ssb.sql
sed -i"" -e "s|PATHVAR|${INSTALL_DIR}|" ./export-ssb-skew.sql
cat ./export-ssb.sql | duckdb ./ssb.duckdb
cat ./export-ssb-skew.sql | duckdb ./ssb_skew.duckdb

sed -i"" -e "s|true|1|g" ./data/ssb/date.tbl
sed -i"" -e "s|false|0|g" ./data/ssb/date.tbl
sed -i"" -e "s|true|1|g" ./data/ssb-skew/date.tbl
sed -i"" -e "s|false|0|g" ./data/ssb-skew/date.tbl
mv data/ postgres_data/

if [[ ! -d "${INSTALL_DIR}/venv" ]]; then
  echo "Creating Python Virtual Environment"
  python3 -m venv venv
  source "venv/bin/activate"
  pip install pip --upgrade > /dev/null
  pip -q install -r requirements.txt
  echo "$HOSTNAME"
fi
