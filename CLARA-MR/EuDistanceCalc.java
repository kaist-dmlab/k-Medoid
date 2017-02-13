package dmlab.main;

import java.util.ArrayList;
import java.util.List;

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
		
		
		return (float) Math.sqrt(result);
	}
	
	
	
	
	public static float CostCalculation(ArrayList<Point> dataSet, Point[] Medoids)
	{
		float newCost = 0;
		for(int i=0; i<dataSet.size(); i++)
		{
			float min = Float.MAX_VALUE;
			for(int j=0; j<Medoids.length; j++)
			{
				float dist = EuDistanceCalc.CalculateDistance(dataSet.get(i), Medoids[j]);
				if(min > dist)
				{
					min = dist;
				}
			}
			newCost += min;
		}
		
		return newCost/(float)dataSet.size();
	}
	
	
	public static float CostCalculation(ArrayList<Point> eachDataSet, Point Medoids)
	{
		float newCost = 0;
		for(int i=0; i<eachDataSet.size(); i++)
		{
			float dist = EuDistanceCalc.CalculateDistance(eachDataSet.get(i), Medoids);
			newCost += dist;
		}
		
		return newCost/(float)eachDataSet.size();
	}	
	
	
	public static float CostCalculation(List<Point> dataSet, List<Point> Medoids)
	{
		float newCost = 0;
		for(int i=0; i<dataSet.size(); i++)
		{
			float min = Float.MAX_VALUE;
			for(int j=0; j<Medoids.size(); j++)
			{
				float dist = EuDistanceCalc.CalculateDistance(dataSet.get(i), Medoids.get(j));
				if(min > dist)
				{
					min = dist;
				}
			}
			newCost += min;
		}
		
		return newCost;
	}
	
	public static double CalcClusterIntraDistance(List<Point> dataSet, List<Point> medoidsSet)
	{
		double cost = 0;
		
		//dataSet is already assinged points.
		for(int i=0; i<dataSet.size(); i++)
		{
			for(int j=0; j<medoidsSet.size(); j++)
			{
				if(medoidsSet.get(j).getClassLabel() == dataSet.get(i).getClassLabel())
				{
					cost += CalculateDistance(dataSet.get(i), medoidsSet.get(j));
				}
			}
			
		}
		return cost;
	}
	
}
