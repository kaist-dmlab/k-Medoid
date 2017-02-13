package dmlab.main;


import java.util.ArrayList;

public class Point {
	private float[] attr;
	private int classLabel = -1;
	
	public Point()
	{

	}
	
	public Point(int size, int classLable)
	{
		attr = new float[size];
		this.classLabel = classLable;
	}

	public float[] getAttr() {
		return attr;
	}

	public int getClassLabel() {
		return classLabel;
	}

	public void setClassLabel(int classLabel) {
		this.classLabel = classLabel;
	}

	public void setAttr(float[] ptrs) {
		this.attr = ptrs;
	}
	
	public void copyPoint(Point point)
	{
		this.classLabel = point.classLabel;
		for(int i=0; i<this.attr.length; i++)
		{
			float temp = point.getAttr()[i];
			this.attr[i] = (temp);
		}
	}
	
	public String toString()
	{
		String result = "";
		
		for(int i=0; i<attr.length; i++)
		{
			if(i != attr.length -1)
				result += attr[i] + ",";
			else
				result += attr[i];
		}
		
		return result;
	}
	
	public boolean isSamePoint(Point other)
	{
		int count = 0;
		
		for(int i=0; i<this.attr.length; i++)
		{
			float[] temp = other.getAttr();
			if(this.attr[i]== temp[i])
			{
				count++;
			}
		}
		
		if(count == this.attr.length)
			return true;
		else 
			return false;
		
	}
}
