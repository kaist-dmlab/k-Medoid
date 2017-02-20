package dmlab.main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
/**
 * @author Hwanjun Song(KAIST), Jae-Gil Lee(KAIST) and Wook-Shin Han(POSTECH)
 * Created on 16/12/01.
 * To find k-medoids using PAMAE algorithm
 **/
public class MainDriver {
	
	public static void main(String[] args) throws IOException
	{
		if(args.length != 6)
		{	
			System.out.println("Usage: PAMAE <Input Path> <# of Clusters> <# of Sampled Objects> <# of Sample> <# of Partition> <# of Iteration>");
			System.exit(1);
		}
		
		//argument
		String inputPath = args[0];
		int numOfClusters = Integer.parseInt(args[1]);
		int numOfSampledObjects = Integer.parseInt(args[2]);
		int numOfSamples = Integer.parseInt(args[3]);
		int numOfCores = Integer.parseInt(args[4]);
		int numOfIteration = Integer.parseInt(args[5]);
		
		//set-up spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("PAMAE");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		//set-up output path
		FileWriter fw = new FileWriter("PAMAE_OUTPUT.txt");
		BufferedWriter bw = new BufferedWriter(fw);
		
		//parsing input file and transform to RDD 
		JavaRDD<FloatPoint> dataSet = PAMAE.readFile(sc, inputPath, numOfCores);
		
	    //Phase I (STEP 1 ~ STEP 3) : Parallel Seeding
	   	List<FloatPoint> bestSeed = PAMAE.PHASE_I(sc, dataSet, numOfClusters, numOfSampledObjects, numOfSamples, numOfCores);
		
	    //Phase II (STEP 4 ~ STEP 5) : Parallel Refinement
	   	//iteration
	   	List<FloatPoint> finalMedoids=null;
	   	for(int i=0; i<numOfIteration; i++)
	   	{
	   		finalMedoids = PAMAE.PHASE_II(sc, dataSet, bestSeed, numOfClusters, numOfSampledObjects, numOfSamples, numOfCores);
	   		
	   		//set new class label for next iteration
	   		for(int j=0; j<finalMedoids.size(); j++)
	   			finalMedoids.get(j).setClassLabel(j);
	   		bestSeed = finalMedoids;   		
	   		
	   		double finalError = PAMAE.FinalError(sc, dataSet, finalMedoids);
			bw.write("["+(i+1)+" iter] CLUSTERING ERROR : " + finalError +"\n");
	   	}
	   	
		bw.write("FINAL K MEDOIDS\n");
	    for(int i=0; i<finalMedoids.size(); i++)
	    	bw.write(finalMedoids.get(i).toString()+"\n");  
	
	    //unpersist dataset and bestseed
	    dataSet.unpersist();
	    bw.close();
	    sc.stop();
	    sc.close();
	}
}
