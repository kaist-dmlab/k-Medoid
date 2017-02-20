#!/bin/bash
for size in {30,60,90,120,150,180,210,240,270,300}
do
	echo "spark-submit --class dmlab.main.MainDriver PAMAE-Spark.jar wasb://dmcluster@dmclusterstg.blob.core.windows.net/dataset/TeraClickLog$size.csv 50 2000 5 40 1 > SCALABILITY/PAMAE-Spark/$size"
	#spark-submit --class dmlab.main.MainDriver PAMAE-Spark.jar wasb://dmcluster@dmclusterstg.blob.core.windows.net/dataset/TeraClickLog$size.csv 50 2000 5 40 1 > SCALABILITY/PAMAE-Spark/$size
done


