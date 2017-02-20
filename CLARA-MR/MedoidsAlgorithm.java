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
	public static float[][] PreCalculate(List<Point> dataSet)
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
	public static void NormalOriginalPAM(List<Point> dataSet, List<Point> medoids, float[][] preCalcResult) {
		List<Point> clusteredPoints;
	
		int iterations = 0;
		List<Point> newMedoids = null; 
		
		do{
			iterations++;
			clusteredPoints = new ArrayList<Point>();
			
			for(Point medoid : medoids) {
				clusteredPoints.add(medoid);
			}
			
			//assing step.
			for(Point p : dataSet) {
				float minDistance = Float.MAX_VALUE;
				for(Point medoid : medoids) {
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
				List<Point> oldMedoids = cloneList(medoids);
				newMedoids = calculateNewMedoids(clusteredPoints, oldMedoids, preCalcResult);
			}
			else {
				newMedoids = calculateNewMedoids(clusteredPoints, newMedoids, preCalcResult);
			}
			
			//System.out.println("Number of Iterations: " + iterations);
		} while(!stopIterations(newMedoids,medoids));
		

	}
	
	
	private static boolean containsPoint(Point[] points, Point point) {
		for(Point p : points) {
			if(p == point) 
				return true;
		}
		
		return false;
	}
	
	private static boolean containsPoint(List<Point> points, Point point) {
		for(Point p : points) {
			if(p == point) 
				return true;
		}
		
		return false;
	}
	
	public static  float getTotalCost(List<Point> clusteredPoints, List<Point> newMedoids, float[][] preCalcResult) {
		float totalCost = 0;
		
		for(Point point : clusteredPoints) {
			float cost = Float.MAX_VALUE;
			
			for(Point newMedoid : newMedoids) {
				float tempCost = preCalcResult[point.getPointNum()][newMedoid.getPointNum()];
				if(tempCost < cost)
					cost = tempCost;
			}
			
			totalCost += cost;
		}
		
		return totalCost;
	}
	
	public static  float getTotalCost(List<Point> clusteredPoints, List<Point> newMedoids) {
		float totalCost = 0;
		
		for(Point point : clusteredPoints) {
			float cost = Float.MAX_VALUE;
			
			for(Point newMedoid : newMedoids) {
				float tempCost = EuDistanceCalc.CalculateDistance(point, newMedoid);
				if(tempCost < cost)
					cost = tempCost;
			}
			
			totalCost += cost;
		}
		
		return totalCost;
	}	
	private static  List<Point> calculateNewMedoids(List<Point> clusteredPoints, List<Point> oldMedoids, float[][] preCalcResult) {
		for(int j = 0; j < oldMedoids.size(); j++) {
			double oldTotalCost = getTotalCost(clusteredPoints, oldMedoids, preCalcResult);
			double newTotalCost = Double.MAX_VALUE;
			
			Point oriMedoid = oldMedoids.get(j);
			Point candidateMedoid = null;
			
			int label = oldMedoids.get(j).getClassLabel();
			List<Point> clusterPoints = new ArrayList<Point>();
			
			for(int i=0; i<clusteredPoints.size(); i++)
				if(clusteredPoints.get(i).getClassLabel()==label)
					clusterPoints.add(clusteredPoints.get(i));
			
			for(Point candidate : clusterPoints) {
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

	private static  boolean stopIterations(List<Point> newMedoids, List<Point> oldMedoids) {
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
	
	
	



	public static List<Point> cloneList(List<Point> dogList) {
	    List<Point> clonedList = new ArrayList<Point>(dogList.size());
	    for (Point pt : dogList) {
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
    public static List<Point> chooseInitialMedoids_ver2(List<Point> patternList, int numOfCluster, float[][] preCalcResult, float percent) {	
		//1. 순차적으로 아무점 할당
		ArrayList<Point> medoids = new ArrayList<Point>();
        for(int i=0; i<numOfCluster; i++)
        	medoids.add(patternList.get(i));
		
        //2. 10%뽑기
      		
        ArrayList<Point> sampleForInit = new ArrayList<>();
        HashSet<Integer> initIndex = new HashSet<Integer>();
        
        while(initIndex.size() != (int)(patternList.size()*percent))
        	initIndex.add((int)(Math.random()*patternList.size()));
        
        Iterator<Integer> it = initIndex.iterator();
		while(it.hasNext())
		{
			 Point medoidPattern = patternList.get(it.next());
			 sampleForInit.add(medoidPattern);
		}
        
        //1바퀴 돌리기
		for(int i=0; i<medoids.size(); i++)
		{
			float oldTotalCost = getTotalCost(sampleForInit, medoids, preCalcResult);
			float newTotalCost = Float.MAX_VALUE;
			
			Point oriMedoid = medoids.get(i);
			Point candidateMedoid = null;
					
			for(Point candidate : sampleForInit) {
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
}
