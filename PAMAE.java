package dmlab.main;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

import dmlab.main.Algorithms;
import dmlab.main.FloatPoint;
import dmlab.main.PAMAE;
import scala.Tuple2;

import scala.Tuple2;

public final class PAMAE {
	//delimiter for parsing .csv file
	public static String eleDivider = ",";
	
	/**
	 * Read and parsing input files
	 * @param sc
	 * @param inputPath
	 * @param numOfCores
	 * @return dataSet
	 */
	public static JavaRDD<FloatPoint> readFile(JavaSparkContext sc, String inputPath, int numOfCores)
	{
	    //read input file(s) and load to RDD
	    JavaRDD<String> lines = sc.textFile(inputPath,numOfCores);
	    JavaRDD<FloatPoint> dataSet = lines.map(new PAMAE.ParsePoint()).persist(StorageLevel.MEMORY_AND_DISK_SER());
	    return dataSet;
	}
	
	/**
	 * Phase I of PAMAE algorithm : parallel seeding
	 * @param sparkcontext
	 * @param inputPath
	 * @param numOfClusters
	 * @param numOfSampledObjects
	 * @param numOfSamples
	 * @param numOfCores
	 * @return bestSeed
	 */
	public static List<FloatPoint> PHASE_I(JavaSparkContext sc, JavaRDD<FloatPoint> dataSet, int numOfClusters, int numOfSampledObjects, int numOfSamples, int numOfCores)
	{   
	    //STEP 1
	    //random sampling with replacement
	    List<FloatPoint> samples = dataSet.takeSample(true, numOfSampledObjects*numOfClusters*10);
	    JavaRDD<FloatPoint> sampleSet = sc.parallelize(samples);
	          
	    //STEP 2 
	    //perform PAM algorithm on the samples
	    List<Tuple2<Integer, List<FloatPoint>>> candidateSet = sampleSet.mapToPair(new PAMAE.Sampling(numOfClusters))
	    													.groupByKey().mapValues(new PAMAE.PAM(numOfSampledObjects, numOfClusters))
	    													.collect();
	    
	    //STEP 3
	    //calculate clustering error of k medoids sets
	    JavaRDD<Tuple2<Integer, List<FloatPoint>>> candidateSetRDD = sc.parallelize(candidateSet).persist(StorageLevel.MEMORY_AND_DISK_SER());
	    List<Tuple2<Integer, Double>> costList = cluteringError(sc, dataSet, candidateSetRDD);   	
	  
	    //select best k medoids set
	    List<Tuple2<Integer, List<FloatPoint>>> candidateList = candidateSetRDD.collect();
	    int finalKey = -1;
	    double phaseIError = Double.MAX_VALUE;
	    for(int i=0; i<costList.size(); i++)
	    {
	    	if(phaseIError > costList.get(i)._2())
	    	{
	    		phaseIError = costList.get(i)._2();
	    		finalKey = costList.get(i)._1();
	    	}
	    } 
	    List<FloatPoint> bestSeed = null;
	    for(int i=0; i<candidateList.size(); i++)
	    {
	    	if(candidateList.get(i)._1() == finalKey)
	    		bestSeed = candidateList.get(i)._2();
	    }
	    
	    System.out.println("PHASE I CLUSTERING ERROR : " + phaseIError+"\n");
		
	    //unpersist candidateset
	    candidateSetRDD.unpersist();
	    
	    return bestSeed;
	}
	
	/**
	 * Phase I of PAMAE algorithm : parallel refinement
	 * @param sc
	 * @param dataSet
	 * @param bestSeed
	 * @param numOfClusters
	 * @param numOfSampledObjects
	 * @param numOfSamples
	 * @param numOfCores
	 * @return final k medoids
	 */
	public static List<FloatPoint> PHASE_II(JavaSparkContext sc, JavaRDD<FloatPoint> dataSet, List<FloatPoint> bestSeed, int numOfClusters, int numOfSampledObjects, int numOfSamples, int numOfCores)
	{
	    
	    //load best seed to RDD
	    JavaRDD<FloatPoint> bestSeedRDD = sc.parallelize(bestSeed);
	    bestSeedRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

	    //STEP 4 & STEP 5 
	    //assign all objects of dataset to closest medoid(seed)
	    List<Tuple2<Integer, List<FloatPoint>>> temp = dataSet.mapToPair(new PAMAE.AssignPoint(numOfCores))
	    													.groupByKey().mapValues(new PAMAE.ModifiedWeiszfeld(bestSeedRDD)).collect();
		 //final k medoids from partitions
		 ArrayList<FloatPoint> finalMedoids = new ArrayList<FloatPoint>();

		 //select best k medoids set among the result of partitions
		 for(int i=0; i<numOfClusters; i++)
		 {
			 int index = -1;
			 double minCost = Double.MAX_VALUE;
			 for(int j=0; j<numOfCores; j++)
			 {
				 if(minCost > temp.get(j)._2().get(i).getCost())
				 {	index = j;
					 minCost = temp.get(j)._2().get(i).getCost();}
			 }
			 finalMedoids.add(temp.get(index)._2().get(i));
			// System.out.println("pt : " + temp.get(index)._2().get(i).toString());
			 
		 }
		bestSeedRDD.unpersist();
	    return finalMedoids;
	}
	
	/**
	 * Calculate clustering error
	 * @param sc
	 * @param dataSet
	 * @param medoid(RDD)
	 * @return clustering error
	 */
	public static List<Tuple2<Integer, Double>> cluteringError(JavaSparkContext sc, JavaRDD<FloatPoint> dataSet, JavaRDD<Tuple2<Integer, List<FloatPoint>>> medoids)
	{
	     List<Tuple2<Integer, Double>> Error = dataSet.flatMapToPair(new PAMAE.CostCaculator(medoids)).
							    								reduceByKey(new Function2<Double, Double, Double>() {
																@Override
																public Double call(Double x, Double y) throws Exception {
																// TODO Auto-generated method stub
																return x+y;
																}
																}).collect(); 
	    return Error;
	}
	
	/**
	 * Get final Error
	 * @param sc
	 * @param dataSet
	 * @param finalMedoids
	 * @return
	 */
	public static double FinalError(JavaSparkContext sc, JavaRDD<FloatPoint> dataSet, List<FloatPoint> finalMedoids)
	{
	    Tuple2<Integer,List<FloatPoint>> medoids = new Tuple2<Integer, List<FloatPoint>>(1,finalMedoids);
	    List<Tuple2<Integer,List<FloatPoint>>> temp = new ArrayList<Tuple2<Integer,List<FloatPoint>>>();
	    temp.add(medoids);  
	    JavaRDD<Tuple2<Integer, List<FloatPoint>>> finalSetRDD = sc.parallelize(temp).persist(StorageLevel.MEMORY_AND_DISK_SER());   
	    List<Tuple2<Integer, Double>> finalError =  cluteringError(sc, dataSet, finalSetRDD);
	    
	    return finalError.get(0)._2;
	}
	
	/**
	 * PsrsePoint
	 * desc : parsing text to Point object.
	 */
	public static class ParsePoint implements Function<String, FloatPoint> {

		@Override
		public FloatPoint call(String line) {
			String[] toks = line.toString().split(eleDivider);
			FloatPoint pt = new FloatPoint(toks.length,-1);
			for(int j=0; j<toks.length; j++)
				pt.getValues()[j] = (Float.parseFloat(toks[j]));
		    return pt;
		}
	}
	
	/**
	 * 
	 * SamplingStep
	 * desc : sampling (parallel) of (sampleNumber) sample .
	 */
	public static class Sampling implements PairFunction<FloatPoint, Integer, FloatPoint> {
		private int parallel = 0;
		
		public Sampling(int parallel) {
			// TODO Auto-generated constructor stub
			this.parallel = parallel;
		}
		
		@Override
		public Tuple2<Integer, FloatPoint> call(FloatPoint value) throws Exception {
		int key = (int)(Math.random()*parallel);
		value.setKey(key);
		return new Tuple2(key,value);
		}
	}
	
	/**
	 * PAM
	 * desc : Partitioning Around Medoid Algorithm
	 */
	public static class PAM implements Function<Iterable<FloatPoint>, List<FloatPoint>>{
		private int sampleNumber = 0;
		private int K = 0;
		
		public PAM( int sampleNumber, int K) {
			// TODO Auto-generated constructor stub
			this.sampleNumber = sampleNumber;
			this.K = K;
		}
		
		@Override
		public List<FloatPoint> call(Iterable<FloatPoint> values) throws Exception {
			// TODO Auto-generated method stub
			List<FloatPoint> sampledDataSet = new ArrayList<FloatPoint>();
			for(FloatPoint pt : values)
				sampledDataSet.add(pt);
			//get sample index;
			HashSet<Integer> sampleIndex = new HashSet<Integer>();
			int dataSize = sampledDataSet.size();
			while(sampleIndex.size() != sampleNumber)
				sampleIndex.add((int)(Math.random()*dataSize));
			//data sampling
			List<FloatPoint> samplePoints = new ArrayList<FloatPoint>();
			for(Integer sample : sampleIndex)
				samplePoints.add(sampledDataSet.get(sample));
			sampledDataSet.clear();
			//pre Calculation
			float[][] preCalcResult = Algorithms.PreCalculate(samplePoints);
			//select initial point
			List<FloatPoint> sampleInit = Algorithms.chooseInitialMedoids(samplePoints, K, preCalcResult, 0.5f);		
			//execute Sampling PAM 
			Algorithms.PAM(samplePoints, sampleInit, preCalcResult);
			//return the result of sample PAM
			return sampleInit;
		}
	}
	
	/**
	 * CostCaculator
	 * desc : calculate clustering error of k medoids sets.
	 */
	public static class CostCaculator implements PairFlatMapFunction<FloatPoint, Integer, Double>{
		List<Tuple2<Integer, List<FloatPoint>>> Candidates;
		
		public CostCaculator(JavaRDD<Tuple2<Integer, List<FloatPoint>>> candidateSetRDD) {
			
			// TODO Auto-generated constructor stub
			Candidates = candidateSetRDD.collect();
		}

		@Override
		public Iterable<Tuple2<Integer, Double>> call(FloatPoint pt)
				throws Exception {
			// TODO Auto-generated method stub
			List<Tuple2<Integer, Double>> output = new ArrayList<Tuple2<Integer, Double>>();
			
			for(int i=0; i<Candidates.size(); i++)
			{
				int key = Candidates.get(i)._1();
				List<FloatPoint> pts = Candidates.get(i)._2();
				
				double min = Double.MAX_VALUE;
				for(int j=0; j<pts.size(); j++)
				{
					double newCost = FunctionSet.distance(pt, pts.get(j));
					if(min > newCost)
						min = newCost;
				}	
				output.add(new Tuple2<Integer, Double>(key, min));
			}
			return output;
		}
	}

	/**
	 * 
	 * AssignPoint
	 * desc : assign random key to each object.
	 */
	public static class AssignPoint implements PairFunction<FloatPoint, Integer, FloatPoint> {
		private int coreNum=-1;
		   		
		public AssignPoint(int coreNum) {
			// TODO Auto-generated constructor stub
		    this.coreNum = coreNum;
		}
		
		@Override
		public Tuple2<Integer, FloatPoint> call(FloatPoint value) throws Exception {
		int key = (int)(Math.random()*coreNum);
		value.setKey(key);
		return new Tuple2(key,value);
		}
	}
	
	/**
	 * 
	 * MedifiedWeiszfeld
	 * desc : weiszfeld algorithm with fine granularity technique
	 */
	public static class ModifiedWeiszfeld implements Function<Iterable<FloatPoint>, List<FloatPoint>>{
		private List<FloatPoint> medoids = null;
		
		public ModifiedWeiszfeld(JavaRDD<FloatPoint> bestSeed) {
			// TODO Auto-generated constructor stub
			this.medoids = bestSeed.collect();
		}
		
		@Override
		public List<FloatPoint> call(Iterable<FloatPoint> values) throws Exception {
			
			// TODO Auto-generated method stub	
			List<FloatPoint> localDataSet = new ArrayList<FloatPoint>();	
			for(FloatPoint pt : values)
				localDataSet.add(pt);
			for(FloatPoint pt: medoids)
				localDataSet.add(pt);	
			List<FloatPoint> finalMedoid = null;
			finalMedoid = Algorithms.refinement(localDataSet, medoids, 0.01);
			
			return finalMedoid;
		}
	}
}
