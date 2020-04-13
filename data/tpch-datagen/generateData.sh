SCALE=$1

cd data/tpch-datagen
if [ ! -d "tpch-dbgen" ]
then
    git clone https://github.com/amirsh/tpch-dbgen.git
    cd tpch-dbgen
    git apply dbgen-fix.diff
    make
else
    echo "tpch-dbgen already exists. Skipping git clone & make"
    cd tpch-dbgen
fi

./dbgen -f -s $SCALE
mkdir ../data
mv *.tbl ../data
