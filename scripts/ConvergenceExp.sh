#!/bin/bash
for dataset in "Covertype.csv" "Census1990.csv" "Cosmo50.csv" "TeraClickLog150.csv"
do
	for i in {1..5}
	do
		#CLAAR-MR' (40+2k,5)
		echo "spark-submit --class dmlab.main.MainDriver PAMAE-Spark.jar wasb://dmcluster@dmclusterstg.blob.core.windows.net/dataset/$dataset 50 140 5 40 10 > CONVERGENCE/CLARA-MR-prime/$dataset/$i"
		spark-submit --class dmlab.main.MainDriver PAMAE-Spark.jar wasb://dmcluster@dmclusterstg.blob.core.windows.net/dataset/$dataset 50 140 5 40 10 > CONVERGENCE/CLARA-MR-prime/$dataset/$i
		#CLARA-MR (100+5k,5)
		echo "spark-submit --class dmlab.main.MainDriver PAMAE-Spark.jar wasb://dmcluster@dmclusterstg.blob.core.windows.net/dataset/$dataset 50 350 5 40 10 > CONVERGENCE/CLARA-MR/$dataset/$i"
		spark-submit --class dmlab.main.MainDriver PAMAE-Spark.jar wasb://dmcluster@dmclusterstg.blob.core.windows.net/dataset/$dataset 50 350 5 40 10 > CONVERGENCE/CLARA-MR/$dataset/$i
		#PAMAE (40k,5)
		echo "spark-submit --class dmlab.main.MainDriver PAMAE-Spark.jar wasb://dmcluster@dmclusterstg.blob.core.windows.net/dataset/$dataset 50 2000 5 40 10 > CONVERGENCE/PPAMAE-Spark/$dataset/$i"
		spark-submit --class dmlab.main.MainDriver PAMAE-Spark.jar wasb://dmcluster@dmclusterstg.blob.core.windows.net/dataset/$dataset 50 2000 5 40 10 > CONVERGENCE/PPAMAE-Spark/$dataset/$i
	done
done



