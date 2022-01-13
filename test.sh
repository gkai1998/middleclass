#!/bin/sh
sh build.sh --clean --lab1 --lab2
cd build/test/lab1/
./lab1_test
cd ../lab2/
./column_sum_test  
./custom_table_test  
./predicated_all_columns_sum_test  
./predicated_column_sum_test  
./predicated_update_test  
./put_get_test