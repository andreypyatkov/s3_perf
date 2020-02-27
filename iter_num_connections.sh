#!/bin/bash

num_conn=(16 24 32 40 48 64 80 96 128)

for n in ${num_conn[@]}; do
	./s3_perf --num_connections=$n $@
	echo ----------------------------------------
done
