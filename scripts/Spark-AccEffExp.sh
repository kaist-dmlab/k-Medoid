#!/bin/bash
for dataset in "Covertype.csv" "Census1990.csv" "Cosmo50.csv" "TeraClickLog150.csv"
do
	for i in {1..5}
	do
		for k in {10,25,50}
		do
			echo "spark-submit --class dmlab.main.MainDriver PAMAE-Spark.jar wasb://dmcluster@dmclusterstg.blob.core.windows.net/dataset/$dataset $k $(($k * 40)) 5 40 1 > PAMAE-Spark/$dataset/$k/$i"
			spark-submit --class dmlab.main.MainDriver PAMAE-Spark.jar wasb://dmcluster@dmclusterstg.blob.core.windows.net/dataset/$dataset $k $(($k * 40)) 5 40 1 > PAMAE-Spark/$dataset/$k/$i
		done
	done
done



