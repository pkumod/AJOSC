mkdir build

cd build

cmake ..

make -j

# Run the program

./ours ${schema_filename} ${query_filename} ${static_stream_filename} ${dynamic_stream_filename} ${primary_key_filename} -l ${#updates_handled_before_start_compute_order_using_AJOSC}



