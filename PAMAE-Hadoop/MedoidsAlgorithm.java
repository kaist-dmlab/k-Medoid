package dmlab.main;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public final class MedoidsAlgorithm {

	
	
	/**
	 * PreCalculate the result of Euclidean
	 * @param dataSet
	 * @return result
	 */
	public static float[][] PreCalculate(List<FloatPoint> dataSet)
	{
		int objectSize = dataSet.size();
		
		for(int i=0; i<objectSize; i++)
			dataSet.get(i).setPointNum(i);
		
		//N object
		float[][] preResult = new float[objectSize][objectSize];
		
		for(int i=0; i<objectSize; i++)
		{
			for(int j=0; j<objectSize; j++)
			{
				preResult[i][j] = EuDistanceCalc.CalculateDistance(dataSet.get(i), dataSet.get(j));
			}
		}
		
		return preResult;
	}
	
	
	/**]
	 * 
	 * @param dataSet : all data points
	 * @param medoids : init medoids points
	 * @return double : final medoids cost
	 */
	public static void NormalOriginalPAM(List<FloatPoint> dataSet, List<FloatPoint> medoids, float[][] preCalcResult) {
		List<FloatPoint> clusteredPoints;
	
		int iterations = 0;
		List<FloatPoint> newMedoids = null; 
		
		do{
			iterations++;
			clusteredPoints = new ArrayList<FloatPoint>();
			
			for(FloatPoint medoid : medoids) {
				clusteredPoints.add(medoid);
			}
			
			//assing step.
			for(FloatPoint p : dataSet) {
				float minDistance = Float.MAX_VALUE;
				for(FloatPoint medoid : medoids) {
					if(!containsPoint(medoids, p)) {
						float distance = EuDistanceCalc.CalculateDistance(p, medoid);
						if(distance < minDistance) {
							minDistance = distance;
							p.setClassLabel(medoid.getClassLabel());
						}
					}
				}
				clusteredPoints.add(p);
			}
			
			if(iterations == 1) {
				List<FloatPoint> oldMedoids = cloneList(medoids);
				newMedoids = calculateNewMedoids(clusteredPoints, oldMedoids, preCalcResult);
			}
			else {
				newMedoids = calculateNewMedoids(clusteredPoints, newMedoids, preCalcResult);
			}
			
			//System.out.println("Number of Iterations: " + iterations);
		} while(!stopIterations(newMedoids,medoids));
		

	}
	
	
	private static boolean containsPoint(FloatPoint[] points, FloatPoint point) {
		for(FloatPoint p : points) {
			if(p == point) 
				return true;
		}
		
		return false;
	}
	
	private static boolean containsPoint(List<FloatPoint> points, FloatPoint point) {
		for(FloatPoint p : points) {
			if(p == point) 
				return true;
		}
		
		return false;
	}
	
	public static  float getTotalCost(List<FloatPoint> clusteredPoints, List<FloatPoint> newMedoids, float[][] preCalcResult) {
		float totalCost = 0;
		
		for(FloatPoint point : clusteredPoints) {
			float cost = Float.MAX_VALUE;
			
			for(FloatPoint newMedoid : newMedoids) {
				float tempCost = preCalcResult[point.getPointNum()][newMedoid.getPointNum()];
				if(tempCost < cost)
					cost = tempCost;
			}
			
			totalCost += cost;
		}
		
		return totalCost;
	}
	
	public static  float getTotalCost(List<FloatPoint> clusteredPoints, List<FloatPoint> newMedoids) {
		float totalCost = 0;
		
		for(FloatPoint point : clusteredPoints) {
			float cost = Float.MAX_VALUE;
			
			for(FloatPoint newMedoid : newMedoids) {
				float tempCost = EuDistanceCalc.CalculateDistance(point, newMedoid);
				if(tempCost < cost)
					cost = tempCost;
			}
			
			totalCost += cost;
		}
		
		return totalCost;
	}	
	private static  List<FloatPoint> calculateNewMedoids(List<FloatPoint> clusteredPoints, List<FloatPoint> oldMedoids, float[][] preCalcResult) {
		for(int j = 0; j < oldMedoids.size(); j++) {
			double oldTotalCost = getTotalCost(clusteredPoints, oldMedoids, preCalcResult);
			double newTotalCost = Double.MAX_VALUE;
			
			FloatPoint oriMedoid = oldMedoids.get(j);
			FloatPoint candidateMedoid = null;
			
			int label = oldMedoids.get(j).getClassLabel();
			List<FloatPoint> clusterPoints = new ArrayList<FloatPoint>();
			
			for(int i=0; i<clusteredPoints.size(); i++)
				if(clusteredPoints.get(i).getClassLabel()==label)
					clusterPoints.add(clusteredPoints.get(i));
			
			for(FloatPoint candidate : clusterPoints) {
				oldMedoids.set(j, candidate);
				double tempTotalCost = getTotalCost(clusteredPoints, oldMedoids, preCalcResult);
				
				if(tempTotalCost < newTotalCost) {
					newTotalCost = tempTotalCost;
					candidateMedoid = candidate;
				}
			}
			
			if(newTotalCost < oldTotalCost) {
				oldMedoids.set(j, candidateMedoid);
			}
			else {
				oldMedoids.set(j, oriMedoid);
			}
		}
	
		return oldMedoids;
	}

	private static  boolean stopIterations(List<FloatPoint> newMedoids, List<FloatPoint> oldMedoids) {
		boolean stopIterations = true;
		
		for(int i = 0; i < newMedoids.size(); i++){
			if(!containsPoint(newMedoids, oldMedoids.get(i))){
				stopIterations = false;
			}
			
			oldMedoids.set(i, newMedoids.get(i));
			oldMedoids.get(i).setClassLabel(i);
		}
		
		return stopIterations;
	}
	
	
	



	public static List<FloatPoint> cloneList(List<FloatPoint> dogList) {
	    List<FloatPoint> clonedList = new ArrayList<FloatPoint>(dogList.size());
	    for (FloatPoint pt : dogList) {
	        clonedList.add(pt);
	    }
	    return clonedList;
	}

	/**
	 * Choose Initial Point ver2
	 * @param patternList : data point
	 * @param numOfCluster : the number of cluster
	 * @param preCalcResult : distance table
	 * @param percent : ratio of sampling for init[0-1]
	 * @return init k medoid list
	 */
    public static List<FloatPoint> chooseInitialMedoids_ver2(List<FloatPoint> patternList, int numOfCluster, float[][] preCalcResult, float percent) {	
		//1. 순차적으로 아무점 할당
		ArrayList<FloatPoint> medoids = new ArrayList<FloatPoint>();
        for(int i=0; i<numOfCluster; i++)
        	medoids.add(patternList.get(i));
		
        //2. 10%뽑기
      		
        ArrayList<FloatPoint> sampleForInit = new ArrayList<>();
        HashSet<Integer> initIndex = new HashSet<Integer>();
        
        while(initIndex.size() != (int)(patternList.size()*percent))
        	initIndex.add((int)(Math.random()*patternList.size()));
        
        Iterator<Integer> it = initIndex.iterator();
		while(it.hasNext())
		{
			 FloatPoint medoidPattern = patternList.get(it.next());
			 sampleForInit.add(medoidPattern);
		}
        
        //1바퀴 돌리기
		for(int i=0; i<medoids.size(); i++)
		{
			float oldTotalCost = getTotalCost(sampleForInit, medoids, preCalcResult);
			float newTotalCost = Float.MAX_VALUE;
			
			FloatPoint oriMedoid = medoids.get(i);
			FloatPoint candidateMedoid = null;
					
			for(FloatPoint candidate : sampleForInit) {
				medoids.set(i,candidate);
				float tempTotalCost = getTotalCost(sampleForInit, medoids, preCalcResult);
				
				if(tempTotalCost < newTotalCost) {
					newTotalCost = tempTotalCost;
					candidateMedoid = candidate;
				}
			}		
			
			if(newTotalCost < oldTotalCost) {
				medoids.set(i,candidateMedoid);
			}
			else {
				medoids.set(i,oriMedoid);
			}
			medoids.get(i).setClassLabel(i);
		}
	
        return medoids;
    }	
	
	/**
	 * PHASE II
	 */
	public static ArrayList<DoublePoint> RefineWeiszfeld(List<DoublePoint> dataSet, List<DoublePoint> k_Medoids, double epsilon)
	{
		//0. init
		ArrayList<DoublePoint> new_K_Medoids = new ArrayList<DoublePoint>();
		//1. assing procedure.
		for(int i=0; i<dataSet.size(); i++)
		{
			double min = Double.MAX_VALUE;
			int classLabel = -1;
			for(int j=0; j<k_Medoids.size(); j++)
			{
				double distance = EuDistanceCalc.CalculateDistance(k_Medoids.get(j), dataSet.get(i));
				if(distance < min)
				{
					classLabel = k_Medoids.get(j).getClassLabel();
					min = distance;
				}
			}
			dataSet.get(i).setClassLabel(classLabel);
		}

		
		//2. new Medoids Calculation.
		//call doFames fuction!. (input : cluster points, return new Medoids.)
		for(int i=0; i<k_Medoids.size(); i++)
		{
			ArrayList<DoublePoint> eachClusterPoints = new ArrayList<DoublePoint>();
			//클러스터셋뽑기.
			for(int j=0; j<dataSet.size(); j++)
			{
				if(dataSet.get(j).getClassLabel() == k_Medoids.get(i).getClassLabel())
				{
					eachClusterPoints.add(dataSet.get(j));
				}
			}	
			
            Input input = new Input();
            input.setDimension(dataSet.get(0).getAttr().length);
            input.setPoints(eachClusterPoints);
            input.setPermissibleError(epsilon);

            WeiszfeldAlgorithm weiszfeld = new WeiszfeldAlgorithm(); 
            Output output = weiszfeld.process(input,k_Medoids.get(i));
            DoublePoint result =  output.getPoint();
			
			float cost = EuDistanceCalc.CostCalculation(eachClusterPoints, result);
			result.setCost(cost);
			
			new_K_Medoids.add(result);
		}
		
		return new_K_Medoids;
	}
	

}
