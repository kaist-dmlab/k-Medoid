package dmlab.main;

import java.util.ArrayList;
import java.util.List;

public final class GREEDI {

	//GREEDI algorithm
	public static List<Point> GREEDI(List<Point> dataSet, int numOfMedoid)
	{
		ArrayList<Point> medoids = new ArrayList<>();
		float minCost = Float.MAX_VALUE;
		
		for(int i=0; i<numOfMedoid; i++)
		{
			int minIndex = -1;
			for(int j=0; j<dataSet.size(); j++)
			{
				Point pt = dataSet.get(j);
				
				if(medoids.contains(pt))
					continue;
				
				medoids.add(pt);
				float newCost = getTotalCost(dataSet, medoids);
				medoids.remove(pt);
				
				if(minCost > newCost)
				{
					minIndex = j;
					minCost = newCost;
				}
					
			}
			//add GREEDI object
			medoids.add(dataSet.get(minIndex));
		}
	
		return medoids;
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
	
}
