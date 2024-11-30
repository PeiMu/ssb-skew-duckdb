INSTALL_DIR="${PWD}"
set -o pipefail

if [[ ! -d "${INSTALL_DIR}/ssb-dbgen" ]]; then
  echo "Downloading SSB DBGen..."
  git clone https://github.com/eyalroz/ssb-dbgen.git
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
rm -rf data

echo "Preparing benchmark data... [Postgres]"
mkdir -p data/ssb
mkdir -p data/ssb-skew


