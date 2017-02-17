#!/bin/bash
hadoop jar PAM-MR.jar dmlab.main.MainDriver /dataset/example_dataset.csv /output 10 > "PAM-MR.txt"
hadoop jar FAMES-MR.jar dmlab.main.MainDriver /dataset/example_dataset.csv /output 10 > "FAMES-MR.txt"
hadoop jar CLARA-MR.jar dmlab.main.MainDriver /dataset/example_dataset.csv /output 5 10 1 > "CLARA-MR.txt"
hadoop jar GREEDI.jar dmlab.main.MainDriver /dataset/example_dataset.csv /output 10 10 > "GREEDI-MR.txt"
# In the case of MR-KMEDIAN, the sameple should be made before running the weighted k-median algorithm
hadoop jar ITERATIVE-SAMPLING.jar dmlab.main.MainDriver /dataset/example_dataset output 10 0.1 10 10000 > "iterative_sample.csv"
hadoop dfs -put iterative_sample.txt /dataset/iterative_sample.csv
hadoop jar WEIGHTED_KMEDIAN.jar dmlab.main.MainDriver /dataset/example_dataset.csv /dataset/iterative_sample.csv output 10 10 > "MR-KMEDIAN.txt"
hadoop jar PAMAE-Hadoop.jar  dmlab.main.MainDriver /dataset/example_dataset.csv /output 400 5 10 10 > "PAMAE-Hadoop.txt"
