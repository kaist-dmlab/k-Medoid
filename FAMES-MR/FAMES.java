package dmlab.main;
 

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;



public class FAMES {

	// poinsInCluster : set of elements S 
	public static Point doFAMES(List<Point> pointsInCluster) {
		int dim = 0;
		if(pointsInCluster.size()>0)
			dim = pointsInCluster.get(0).getAttr().length;
		
		// 1. randomly choose an element s_z in S, pointsInCluster
		Random generator = new Random();
		Point s_z = pointsInCluster.get(generator.nextInt(pointsInCluster.size())); //아무 클러스터 점이나뽑음
		
		// 2. find the farthest element s1' from s_z
		float tempFarthestDistance1 = Float.MIN_VALUE;
		int tempIndex1 = 0;
		
		for(int i = 0; i < pointsInCluster.size(); i++) {
			float tempDistance = EuDistanceCalc.CalculateDistance(s_z, pointsInCluster.get(i));
			
			if(tempDistance > tempFarthestDistance1) {
				tempFarthestDistance1 = tempDistance;
				tempIndex1 = i; 
			}
			else {
				continue;
			}
		}
		
		Point s1 = pointsInCluster.get(tempIndex1);
		
		// 3. find the farthest element s1'' from s1' 
		float tempFarthestDistance2 = Float.MIN_VALUE;
		int tempIndex2 = 0; 
		
		for(int i = 0; i < pointsInCluster.size(); i++) {
			float tempDistance = EuDistanceCalc.CalculateDistance(s1, pointsInCluster.get(i));
			
			if(tempDistance > tempFarthestDistance2) {
				tempFarthestDistance2 = tempDistance;
				tempIndex2 = i; 
			}
			else {
				continue; 
			}
		}
		
		Point s2 = pointsInCluster.get(tempIndex2);
		
		// 4. let p_m be the projection of element s_md over line (s1', s1'') and m = x_md be its distance to s1' 
		ArrayList<Float> xDistancesSorted = new ArrayList<Float>();
		
		for(int i = 0; i < pointsInCluster.size(); i++) {
			float tempResult = EuDistanceCalc.CalculateDistance(s1, pointsInCluster.get(i)) + 
					EuDistanceCalc.CalculateDistance(s1, s2) - 
					EuDistanceCalc.CalculateDistance(s2, pointsInCluster.get(i));
			
			xDistancesSorted.add(tempResult / (float)(2 * Math.sqrt(EuDistanceCalc.CalculateDistance(s1, s2))));
		}
		
		Collections.sort(xDistancesSorted);
		
		int middle = xDistancesSorted.size() / 2; 	//미디안 포인트르 ㄹ잡음.
		float x_md = 0;	//미디안포인트 인덱스.
		
		if(xDistancesSorted.size() % 2 == 1) {				//사이즈가 홀수이면 가운대것
			x_md = xDistancesSorted.get(middle);
		}
		else {
			x_md = xDistancesSorted.get(middle-1);		//아니면 가운데에서 앞에꺼.
		}
		
		
		
		
		
		//dataset s'1, a'2, dist(x_md)
		ArrayList<Point> s1_Set = new ArrayList<Point>();
		ArrayList<Point> s2_Set = new ArrayList<Point>();
		ArrayList<Float> x_md_Set = new ArrayList<Float>();
		
		//init
		s1_Set.add(s1);
		s2_Set.add(s2);
		x_md_Set.add(x_md);
		float chord = (float)Math.sqrt(EuDistanceCalc.CalculateDistance(s1_Set.get(0), s2_Set.get(0)));

		// 4. iteration g from to f
		if(dim > 1)
		{
			for(int g=1; g<dim; g++)
			{
				float tempDistance = Float.MAX_VALUE;
				int index = -1;
			
				//get new s1
				for(int i = 0; i < pointsInCluster.size(); i++) 
				{
					float curDistance = 0;
					for(int h=0; h<=g-1; h++)
					{
						curDistance += Math.abs(chord-Math.sqrt(EuDistanceCalc.CalculateDistance(s1_Set.get(h), pointsInCluster.get(i))))
								+ Math.abs(chord-Math.sqrt(EuDistanceCalc.CalculateDistance(s2_Set.get(h), pointsInCluster.get(i))));
					}
				
					if(tempDistance > curDistance)
					{
						tempDistance = curDistance;
						index = i;
					}
					
				}
			
				s1_Set.add(pointsInCluster.get(index));
				
				//get new s2
				
				tempDistance = Float.MAX_VALUE;
				index = -1;
				
				for(int i = 0; i < pointsInCluster.size(); i++) 
				{
					float curDistance = 0;
					for(int h=0; h<g; h++)
					{
						curDistance += Math.abs(chord-Math.sqrt(EuDistanceCalc.CalculateDistance(s1_Set.get(h), pointsInCluster.get(i))))
								+ Math.abs(chord-Math.sqrt(EuDistanceCalc.CalculateDistance(s2_Set.get(h), pointsInCluster.get(i))));
					}
					
					curDistance += Math.abs(chord-Math.sqrt(EuDistanceCalc.CalculateDistance(s1_Set.get(g), pointsInCluster.get(i))));
					
					if(tempDistance > curDistance)
					{
						tempDistance = curDistance;
						index = i;
					}
				}
				
				s2_Set.add(pointsInCluster.get(index));
			
				
				
				//get x_md
				ArrayList<Float> sortedDistance = new ArrayList<Float>();
				
				for(int i = 0; i < pointsInCluster.size(); i++) {
					float tempResult = EuDistanceCalc.CalculateDistance(s1_Set.get(g), pointsInCluster.get(i)) + 
							EuDistanceCalc.CalculateDistance(s1_Set.get(g), s2_Set.get(g)) - 
							EuDistanceCalc.CalculateDistance(s2_Set.get(g), pointsInCluster.get(i));
					
					sortedDistance.add(tempResult / (float)(2 * Math.sqrt(EuDistanceCalc.CalculateDistance(s1_Set.get(g), s2_Set.get(g)))));
				}
				
				
				Collections.sort(sortedDistance);
				
				int mid = sortedDistance.size() / 2; 	//미디안 포인트르 ㄹ잡음.
				float new_x_md = 0;	//미디안포인트 인덱스.
				
				if(sortedDistance.size() % 2 == 1) {				//사이즈가 홀수이면 가운대것
					new_x_md = sortedDistance.get(mid);
				}
				else {
					new_x_md = sortedDistance.get(mid-1);		//아니면 가운데에서 앞에꺼.
				}
				
				x_md_Set.add(new_x_md);
			}
		}
		
		
	
		
		
		//select the medoid r
		float tempDistance = Float.MAX_VALUE;
		int index = -1;
		for(int i = 0; i < pointsInCluster.size(); i++) 
		{
			float curDistance = 0;
			
			for(int g=0; g<s1_Set.size(); g++)
			{
				curDistance += Math.abs(Math.sqrt(EuDistanceCalc.CalculateDistance(s1_Set.get(g), pointsInCluster.get(i)))-x_md_Set.get(g))
						+ Math.abs(Math.sqrt(EuDistanceCalc.CalculateDistance(s2_Set.get(g), pointsInCluster.get(i)))-  
								(Math.sqrt(EuDistanceCalc.CalculateDistance(s1_Set.get(g), s2_Set.get(g)))-x_md_Set.get(g)));
			}
			if(tempDistance > curDistance)
			{
				
				tempDistance = curDistance;
				index = i;
			}
		}
		
		return pointsInCluster.get(index);

	}
	
}
