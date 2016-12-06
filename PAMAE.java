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
import dmlab.main.DoublePoint;
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
	public static JavaRDD<DoublePoint> readFile(JavaSparkContext sc, String inputPath, int numOfCores)
	{
	    //read input file(s) and load to RDD
	    JavaRDD<String> lines = sc.textFile(inputPath,numOfCores);
	    JavaRDD<DoublePoint> dataSet = lines.map(new PAMAE.ParsePoint()).persist(StorageLevel.MEMORY_AND_DISK_SER());
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
	public static List<DoublePoint> PHASE_I(JavaSparkContext sc, JavaRDD<DoublePoint> dataSet, int numOfClusters, int numOfSampledObjects, int numOfSamples, int numOfCores)
	{   
	    //STEP 1
	    //random sampling with replacement
	    List<DoublePoint> samples = dataSet.takeSample(true, numOfSampledObjects*numOfClusters*10);
	    JavaRDD<DoublePoint> sampleSet = sc.parallelize(samples);
	          
	    //STEP 2 
	    //perform PAM algorithm on the samples
	    List<Tuple2<Integer, List<DoublePoint>>> candidateSet = sampleSet.mapToPair(new PAMAE.Sampling(numOfClusters))
	    													.groupByKey().mapValues(new PAMAE.PAM(numOfSampledObjects, numOfClusters))
	    													.collect();
	    
	    //STEP 3
	    //calculate clustering error of k medoids sets
	    JavaRDD<Tuple2<Integer, List<DoublePoint>>> candidateSetRDD = sc.parallelize(candidateSet).persist(StorageLevel.MEMORY_AND_DISK_SER());
	    List<Tuple2<Integer, Double>> costList = cluteringError(sc, dataSet, candidateSetRDD);   	
	  
	    //select best k medoids set
	    List<Tuple2<Integer, List<DoublePoint>>> candidateList = candidateSetRDD.collect();
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
	    List<DoublePoint> bestSeed = null;
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
	public static List<DoublePoint> PHASE_II(JavaSparkContext sc, JavaRDD<DoublePoint> dataSet, List<DoublePoint> bestSeed, int numOfClusters, int numOfSampledObjects, int numOfSamples, int numOfCores)
	{
	    
	    //load best seed to RDD
	    JavaRDD<DoublePoint> bestSeedRDD = sc.parallelize(bestSeed);
	    bestSeedRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

	    //STEP 4 & STEP 5 
	    //assign all objects of dataset to closest medoid(seed)
	    List<Tuple2<Integer, List<DoublePoint>>> temp = dataSet.mapToPair(new PAMAE.AssignPoint(numOfCores))
	    													.groupByKey().mapValues(new PAMAE.ModifiedWeiszfeld(bestSeedRDD)).collect();
		 //final k medoids from partitions
		 ArrayList<DoublePoint> finalMedoids = new ArrayList<DoublePoint>();

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
	public static List<Tuple2<Integer, Double>> cluteringError(JavaSparkContext sc, JavaRDD<DoublePoint> dataSet, JavaRDD<Tuple2<Integer, List<DoublePoint>>> medoids)
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
	public static double FinalError(JavaSparkContext sc, JavaRDD<DoublePoint> dataSet, List<DoublePoint> finalMedoids)
	{
	    Tuple2<Integer,List<DoublePoint>> medoids = new Tuple2<Integer, List<DoublePoint>>(1,finalMedoids);
	    List<Tuple2<Integer,List<DoublePoint>>> temp = new ArrayList<Tuple2<Integer,List<DoublePoint>>>();
	    temp.add(medoids);  
	    JavaRDD<Tuple2<Integer, List<DoublePoint>>> finalSetRDD = sc.parallelize(temp).persist(StorageLevel.MEMORY_AND_DISK_SER());   
	    List<Tuple2<Integer, Double>> finalError =  cluteringError(sc, dataSet, finalSetRDD);
	    
	    return finalError.get(0)._2;
	}
	
	/**
	 * PsrsePoint
	 * desc : parsing text to Point object.
	 */
	public static class ParsePoint implements Function<String, DoublePoint> {

		@Override
		public DoublePoint call(String line) {
			String[] toks = line.toString().split(eleDivider);
			DoublePoint pt = new DoublePoint(toks.length,-1);
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
	public static class Sampling implements PairFunction<DoublePoint, Integer, DoublePoint> {
		private int parallel = 0;
		
		public Sampling(int parallel) {
			// TODO Auto-generated constructor stub
			this.parallel = parallel;
		}
		
		@Override
		public Tuple2<Integer, DoublePoint> call(DoublePoint value) throws Exception {
		int key = (int)(Math.random()*parallel);
		value.setKey(key);
		return new Tuple2(key,value);
		}
	}
	
	/**
	 * PAM
	 * desc : Partitioning Around Medoid Algorithm
	 */
	public static class PAM implements Function<Iterable<DoublePoint>, List<DoublePoint>>{
		private int sampleNumber = 0;
		private int K = 0;
		
		public PAM( int sampleNumber, int K) {
			// TODO Auto-generated constructor stub
			this.sampleNumber = sampleNumber;
			this.K = K;
		}
		
		@Override
		public List<DoublePoint> call(Iterable<DoublePoint> values) throws Exception {
			// TODO Auto-generated method stub
			List<DoublePoint> sampledDataSet = new ArrayList<DoublePoint>();
			for(DoublePoint pt : values)
				sampledDataSet.add(pt);
			//get sample index;
			HashSet<Integer> sampleIndex = new HashSet<Integer>();
			int dataSize = sampledDataSet.size();
			while(sampleIndex.size() != sampleNumber)
				sampleIndex.add((int)(Math.random()*dataSize));
			//data sampling
			List<DoublePoint> samplePoints = new ArrayList<DoublePoint>();
			for(Integer sample : sampleIndex)
				samplePoints.add(sampledDataSet.get(sample));
			sampledDataSet.clear();
			//pre Calculation
			float[][] preCalcResult = Algorithms.PreCalculate(samplePoints);
			//select initial point
			List<DoublePoint> sampleInit = Algorithms.chooseInitialMedoids(samplePoints, K, preCalcResult, 0.5f);		
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
	public static class CostCaculator implements PairFlatMapFunction<DoublePoint, Integer, Double>{
		List<Tuple2<Integer, List<DoublePoint>>> Candidates;
		
		public CostCaculator(JavaRDD<Tuple2<Integer, List<DoublePoint>>> candidateSetRDD) {
			
			// TODO Auto-generated constructor stub
			Candidates = candidateSetRDD.collect();
		}

		@Override
		public Iterable<Tuple2<Integer, Double>> call(DoublePoint pt)
				throws Exception {
			// TODO Auto-generated method stub
			List<Tuple2<Integer, Double>> output = new ArrayList<Tuple2<Integer, Double>>();
			
			for(int i=0; i<Candidates.size(); i++)
			{
				int key = Candidates.get(i)._1();
				List<DoublePoint> pts = Candidates.get(i)._2();
				
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
	public static class AssignPoint implements PairFunction<DoublePoint, Integer, DoublePoint> {
		private int coreNum=-1;
		   		
		public AssignPoint(int coreNum) {
			// TODO Auto-generated constructor stub
		    this.coreNum = coreNum;
		}
		
		@Override
		public Tuple2<Integer, DoublePoint> call(DoublePoint value) throws Exception {
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
	public static class ModifiedWeiszfeld implements Function<Iterable<DoublePoint>, List<DoublePoint>>{
		private List<DoublePoint> medoids = null;
		
		public ModifiedWeiszfeld(JavaRDD<DoublePoint> bestSeed) {
			// TODO Auto-generated constructor stub
			this.medoids = bestSeed.collect();
		}
		
		@Override
		public List<DoublePoint> call(Iterable<DoublePoint> values) throws Exception {
			
			// TODO Auto-generated method stub	
			List<DoublePoint> localDataSet = new ArrayList<DoublePoint>();	
			for(DoublePoint pt : values)
				localDataSet.add(pt);
			for(DoublePoint pt: medoids)
				localDataSet.add(pt);	
			List<DoublePoint> finalMedoid = null;
			finalMedoid = Algorithms.refinement(localDataSet, medoids, 0.01);
			
			return finalMedoid;
		}
	}
}
