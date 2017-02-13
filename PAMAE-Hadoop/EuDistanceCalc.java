package dmlab.main;

import java.util.ArrayList;
import java.util.List;

public final class EuDistanceCalc {

	public EuDistanceCalc()
	{	
	}

	public static float CalculateDistance(FloatPoint a1, FloatPoint a2)
	{
		float result = 0.0f;
	
		for(int i=0; i < a1.getAttr().length; i++)
		{		
			result += (a1.getAttr()[i]-a2.getAttr()[i]) * (a1.getAttr()[i]-a2.getAttr()[i]);		
		}
		
		
		return (float)Math.sqrt(result);
	}
	
	public static float CalculateDistance(DoublePoint a1, DoublePoint a2)
	{
		float result = 0.0f;
	
		for(int i=0; i < a1.getAttr().length; i++)
		{		
			result += (a1.getAttr()[i]-a2.getAttr()[i]) * (a1.getAttr()[i]-a2.getAttr()[i]);		
		}
		
		
		return (float)Math.sqrt(result);
	}
	
/*	public static float CalculateDistance(Point a1, Point a2)
	{
		float result = 0.0f;
	
		for(int i=0; i < a1.getAttr().length; i++)
		{		
			if(a1.getAttr()[i] != a2.getAttr()[i])
				result += 1;
		}
		
		
		return (result);
	}
*/	
	
	public static float CostCalculation(ArrayList<DoublePoint> dataSet, DoublePoint[] Medoids)
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
	
	
	public static float CostCalculation(ArrayList<DoublePoint> eachDataSet, DoublePoint Medoids)
	{
		float newCost = 0;
		for(int i=0; i<eachDataSet.size(); i++)
		{
			float dist = EuDistanceCalc.CalculateDistance(eachDataSet.get(i), Medoids);
			newCost += dist;
		}
		
		return newCost/(float)eachDataSet.size();
	}	
	
	
	public static float CostCalculation(List<FloatPoint> dataSet, List<FloatPoint> Medoids)
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
	
	public static double CalcClusterIntraDistance(List<FloatPoint> dataSet, List<FloatPoint> medoidsSet)
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
