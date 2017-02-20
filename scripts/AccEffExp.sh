#!/bin/bash
size=0
for dataset in "Covertype.csv" "Census1990.csv" "Cosmo50.csv" "TeraClickLog150"
do
	if [ "$dataset" == "Covertype.csv" ]; then
		size=581102
	fi
	if [ "$dataset" == "Census1990.csv" ]; then
		size=2458285
	fi	
	if [ "$dataset" == "Cosmo50.csv" ]; then
		size=315086245
	fi
	if [ "$dataset" == "TeraClickLog150.csv" ]; then
		size=4373472329
	fi
	
	for i in {1..5}
	do
		for k in {10,25,50}
		do
			echo "hadoop jar PAM-MR.jar dmlab.main.MainDriver /dataset/$dataset /output $k > PAM-MR/$dataset/$k/$i"
			echo "hadoop jar FAMES-MR.jar dmlab.main.MainDriver /dataset/$dataset /output $k > FAMES-MR/$dataset/$k/$i"
			echo "hadoop jar CLARA-MR.jar dmlab.main.MainDriver /dataset/$dataset /output 5 $k 0 > CLARA-MR-PRIME/$dataset/$k/$i"
			echo "hadoop jar CLARA-MR.jar dmlab.main.MainDriver /dataset/$dataset /output 5 $k 0 > CLARA-MR/$dataset/$k/$i"
			echo "hadoop jar GREEDI.jar dmlab.main.MainDriver /dataset/$dataset /output $k $k > GREEDI/$dataset/$k/$i"
			echo "hadoop jar ITERATIVE-SAMPLING.jar dmlab.main.MainDriver /dataset/$dataset output $k 0.1 40 $size > iterative_sample.csv"
			echo "hadoop dfs -rm /dataset/iterative_sample.csv"
			echo "hadoop dfs -put iterative_sample.txt /dataset/iterative_sample.csv"
			echo "hadoop jar WEIGHTED_KMEDIAN.jar dmlab.main.MainDriver /dataset/$dataset /dataset/iterative_sample.csv output $k 40 > MR-KMEDIAN/$dataset/$k/$i"
			echo "hadoop jar PAMAE-Hadoop.jar  dmlab.main.MainDriver /dataset/$dataset /output $(($k * 40)) 5 $k 40 > PAMAE-Hadoop/$dataset/$k/$i"
		done
	done
done


