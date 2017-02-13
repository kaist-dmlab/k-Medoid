package dmlab.main;


import java.util.ArrayList;

public final class EuDistanceCalc {

	public EuDistanceCalc()
	{	
	}

	public static float CalculateDistance(Point a1, Point a2)
	{
		float result = 0;
	
		for(int i=0; i < a1.getAttr().length; i++)
		{		
			result += (a1.getAttr()[i]-a2.getAttr()[i]) * (a1.getAttr()[i]-a2.getAttr()[i]);		
		}
		
		
		return (float)Math.sqrt(result);
	}
	
	
	
	
	public static double CostCalculation(ArrayList<Point> dataSet, Point[] Medoids)
	{
		double newCost = 0;
		for(int i=0; i<dataSet.size(); i++)
		{
			double min = Double.MAX_VALUE;
			for(int j=0; j<Medoids.length; j++)
			{
				double dist = EuDistanceCalc.CalculateDistance(dataSet.get(i), Medoids[j]);
				if(min > dist)
				{
					min = dist;
				}
			}
			newCost += min;
		}
		
		return newCost;
	}
	
}
