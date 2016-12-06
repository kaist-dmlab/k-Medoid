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
	public static float[][] PreCalculate(List<DoublePoint> dataSet)
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
	public static void PAM(List<DoublePoint> dataSet, List<DoublePoint> medoids, float[][] preCalcResult) {
		List<DoublePoint> clusteredPoints;
		int iterations = 0;
		List<DoublePoint> newMedoids = null; 
		do{
			iterations++;
			clusteredPoints = new ArrayList<DoublePoint>();
			for(DoublePoint medoid : medoids) 
				clusteredPoints.add(medoid);
			
			//assing step.
			for(DoublePoint p : dataSet) {
				float minDistance = Float.MAX_VALUE;
				for(DoublePoint medoid : medoids) {
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
				List<DoublePoint> oldMedoids = cloneList(medoids);
				newMedoids = calculateNewMedoids(clusteredPoints, oldMedoids, preCalcResult);
			}
			else {
				newMedoids = calculateNewMedoids(clusteredPoints, newMedoids, preCalcResult);
			}
		} while(!stopIterations(newMedoids,medoids));
	}
	
	private static boolean containsPoint(List<DoublePoint> points, DoublePoint point) {
		for(DoublePoint p : points) {
			if(p == point) 
				return true;
		}
		return false;
	}
	

	
	public static  float getTotalCost(List<DoublePoint> clusteredPoints, List<DoublePoint> newMedoids) {
		float totalCost = 0;
		
		for(DoublePoint point : clusteredPoints) {
			float cost = Float.MAX_VALUE;
			
			for(DoublePoint newMedoid : newMedoids) {
				float tempCost = FunctionSet.distance(point, newMedoid);
				if(tempCost < cost)
					cost = tempCost;
			}
			
			totalCost += cost;
		}
		
		return totalCost;
	}	
	private static  List<DoublePoint> calculateNewMedoids(List<DoublePoint> clusteredPoints, List<DoublePoint> oldMedoids, float[][] preCalcResult) {
		for(int j = 0; j < oldMedoids.size(); j++) {
			double oldTotalCost = FunctionSet.clusteringError(clusteredPoints, oldMedoids, preCalcResult);
			double newTotalCost = Double.MAX_VALUE;
			
			DoublePoint oriMedoid = oldMedoids.get(j);
			DoublePoint candidateMedoid = null;
			
			int label = oldMedoids.get(j).getClassLabel();
			List<DoublePoint> clusterPoints = new ArrayList<DoublePoint>();
			
			for(int i=0; i<clusteredPoints.size(); i++)
				if(clusteredPoints.get(i).getClassLabel()==label)
					clusterPoints.add(clusteredPoints.get(i));
			
			for(DoublePoint candidate : clusterPoints) {
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

	private static  boolean stopIterations(List<DoublePoint> newMedoids, List<DoublePoint> oldMedoids) {
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
	
	
	



	public static List<DoublePoint> cloneList(List<DoublePoint> dogList) {
	    List<DoublePoint> clonedList = new ArrayList<DoublePoint>(dogList.size());
	    for (DoublePoint pt : dogList) {
	        clonedList.add(pt);
	    }
	    return clonedList;
	}

	   public static List<DoublePoint> chooseInitialMedoids(List<DoublePoint> patternList, int numOfCluster, float[][] preCalcResult, float percent) {	
			ArrayList<DoublePoint> medoids = new ArrayList<DoublePoint>();
	        for(int i=0; i<numOfCluster; i++)
	        	medoids.add(patternList.get(i));
	      		
	        ArrayList<DoublePoint> sampleForInit = new ArrayList<>();
	        HashSet<Integer> initIndex = new HashSet<Integer>();
	        
	        while(initIndex.size() != (int)(patternList.size()*0.2))
	        	initIndex.add((int)(Math.random()*patternList.size()));
	        
	        Iterator<Integer> it = initIndex.iterator();
			while(it.hasNext())
			{
				DoublePoint medoidPattern = patternList.get(it.next());
				 sampleForInit.add(medoidPattern);
			}
	        
			for(int i=0; i<medoids.size(); i++)
			{
				float oldTotalCost = FunctionSet.clusteringError(sampleForInit, medoids, preCalcResult);
				float newTotalCost = Float.MAX_VALUE;
				
				DoublePoint oriMedoid = medoids.get(i);
				DoublePoint candidateMedoid = null;
						
				for(DoublePoint candidate : sampleForInit) {
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
	public static ArrayList<DoublePoint> refinement(List<DoublePoint> dataSet, List<DoublePoint> k_Medoids, double epsilon)
	{
		
		ArrayList<DoublePoint> new_K_Medoids = new ArrayList<DoublePoint>();
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
			ArrayList<DoublePoint> eachClusterPoints = new ArrayList<DoublePoint>();
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
            DoublePoint result =  output.getPoint();
			
			float cost = FunctionSet.localClusteringError(eachClusterPoints, result);
			result.setCost(cost);
			
			new_K_Medoids.add(result);
		}

		return new_K_Medoids;
	}
	
}
