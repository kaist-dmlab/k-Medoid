package dmlab.main;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public final class Algorithms {
	/**
	 * pre-calculate the result of distance for each pair
	 * @param dataSet
	 * @return pre-calculation distance
	 */
	public static float[][] PreCalculate(List<FloatPoint> dataSet)
	{
		int objectSize = dataSet.size();
		
		for(int i=0; i<objectSize; i++)
			dataSet.get(i).setDimension(i);
		
		//N object
		float[][] preResult = new float[objectSize][objectSize];
		
		for(int i=0; i<objectSize; i++)
		{
			for(int j=0; j<objectSize; j++)
			{
				preResult[i][j] = FunctionSet.distance(dataSet.get(i), dataSet.get(j));
			}
		}
		
		return preResult;
	}
	
	
	/**
	 * PAM algorithm
	 * @param dataSet
	 * @param initial medoids
	 * @param pre-calculation result
	 * @return k medoids
	 */
	public static void PAM(List<FloatPoint> dataSet, List<FloatPoint> medoids, float[][] preCalcResult) {
		List<FloatPoint> clusteredPoints;
		int iterations = 0;
		List<FloatPoint> newMedoids = null; 
		do{
			iterations++;
			clusteredPoints = new ArrayList<FloatPoint>();
			for(FloatPoint medoid : medoids) 
				clusteredPoints.add(medoid);
			
			//assing step.
			for(FloatPoint p : dataSet) {
				float minDistance = Float.MAX_VALUE;
				for(FloatPoint medoid : medoids) {
					if(!containsPoint(medoids, p)) {
						float distance = FunctionSet.distance(p, medoid);
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
		} while(!stopIterations(newMedoids,medoids));
	}
	
	private static boolean containsPoint(List<FloatPoint> points, FloatPoint point) {
		for(FloatPoint p : points) {
			if(p == point) 
				return true;
		}
		return false;
	}
	

	
	public static  float getTotalCost(List<FloatPoint> clusteredPoints, List<FloatPoint> newMedoids) {
		float totalCost = 0;
		
		for(FloatPoint point : clusteredPoints) {
			float cost = Float.MAX_VALUE;
			
			for(FloatPoint newMedoid : newMedoids) {
				float tempCost = FunctionSet.distance(point, newMedoid);
				if(tempCost < cost)
					cost = tempCost;
			}
			
			totalCost += cost;
		}
		
		return totalCost;
	}	
	private static  List<FloatPoint> calculateNewMedoids(List<FloatPoint> clusteredPoints, List<FloatPoint> oldMedoids, float[][] preCalcResult) {
		for(int j = 0; j < oldMedoids.size(); j++) {
			double oldTotalCost = FunctionSet.clusteringError(clusteredPoints, oldMedoids, preCalcResult);
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
				double tempTotalCost = FunctionSet.clusteringError(clusteredPoints, oldMedoids, preCalcResult);
				
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

	   public static List<FloatPoint> chooseInitialMedoids(List<FloatPoint> patternList, int numOfCluster, float[][] preCalcResult, float percent) {	
			ArrayList<FloatPoint> medoids = new ArrayList<FloatPoint>();
	        for(int i=0; i<numOfCluster; i++)
	        	medoids.add(patternList.get(i));
	      		
	        ArrayList<FloatPoint> sampleForInit = new ArrayList<>();
	        HashSet<Integer> initIndex = new HashSet<Integer>();
	        
	        while(initIndex.size() != (int)(patternList.size()*0.2))
	        	initIndex.add((int)(Math.random()*patternList.size()));
	        
	        Iterator<Integer> it = initIndex.iterator();
			while(it.hasNext())
			{
				FloatPoint medoidPattern = patternList.get(it.next());
				 sampleForInit.add(medoidPattern);
			}
	        
			for(int i=0; i<medoids.size(); i++)
			{
				float oldTotalCost = FunctionSet.clusteringError(sampleForInit, medoids, preCalcResult);
				float newTotalCost = Float.MAX_VALUE;
				
				FloatPoint oriMedoid = medoids.get(i);
				FloatPoint candidateMedoid = null;
						
				for(FloatPoint candidate : sampleForInit) {
					medoids.set(i,candidate);
					float tempTotalCost = FunctionSet.clusteringError(sampleForInit, medoids, preCalcResult);
					
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
	 * refinement algorithm
	 * @param dataSet
	 * @param medoids
	 * @param epsilon(error)
	 * @return updated medoids
	 */
	public static ArrayList<FloatPoint> refinement(List<FloatPoint> dataSet, List<FloatPoint> k_Medoids, double epsilon)
	{
		
		ArrayList<FloatPoint> new_K_Medoids = new ArrayList<FloatPoint>();
		for(int i=0; i<dataSet.size(); i++)
		{
			double min = Double.MAX_VALUE;
			int classLabel = -1;
			for(int j=0; j<k_Medoids.size(); j++)
			{
				double distance = FunctionSet.distance(k_Medoids.get(j), dataSet.get(i));
				if(distance < min)
				{
					classLabel = k_Medoids.get(j).getClassLabel();
					min = distance;
				}
			}
			dataSet.get(i).setClassLabel(classLabel);
		}

		for(int i=0; i<k_Medoids.size(); i++)
		{
			ArrayList<FloatPoint> eachClusterPoints = new ArrayList<FloatPoint>();
			for(int j=0; j<dataSet.size(); j++)
			{
				if(dataSet.get(j).getClassLabel() == k_Medoids.get(i).getClassLabel())
				{
					eachClusterPoints.add(dataSet.get(j));
				}
			}	
			
            Input input = new Input();
            input.setDimension(dataSet.get(0).getValues().length);
            input.setPoints(eachClusterPoints);
            input.setPermissibleError(epsilon);

            WeiszfeldAlgorithm weiszfeld = new WeiszfeldAlgorithm(); 
            Output output = weiszfeld.process(input,k_Medoids.get(i));
            FloatPoint result =  output.getPoint();
			
			float cost = FunctionSet.localClusteringError(eachClusterPoints, result);
			result.setCost(cost);
			
			new_K_Medoids.add(result);
		}

		return new_K_Medoids;
	}
	
}
