
# Publication
Xinyi Ye, Xiangyang Gou, Lei Zou, and Wenjie Zhang. 2025. AJOSC: Adaptive Join Order Selection for Continuous Queries. Proc. ACM Manag. Data 3, 3 (SIGMOD), Article 126 (June 2025), 27 pages. https://doi.org/10.1145/3725263

# Dataset
JOB. https://github.com/gregrahn/join-order-benchmark
LDBC-SNB. https://github.com/ldbc/ldbc_snb_datagen_spark
JCC-H. https://github.com/ldbc/dbgen.JCC-H


# Compile
mkdir build

cd build

cmake ..

make -j

# Run

./ours ${schema_filename} ${query_filename} ${static_stream_filename} ${dynamic_stream_filename} ${primary_key_filename} -l ${#updates_handled_before_start_compute_order_using_AJOSC}



