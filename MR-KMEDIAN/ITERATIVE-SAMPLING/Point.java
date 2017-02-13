package dmlab.main;

import java.io.Serializable;
import java.util.ArrayList;

public class Point implements Serializable{
	private float[] attr;
	private int classLabel = -1;
	private int key = -1;
	private int pointNum = -1;
	private float cost = -1;
	
	public int getPointNum() {
		return pointNum;
	}

	public void setPointNum(int pointNum) {
		this.pointNum = pointNum;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}
	
	public float getCost() {
		return cost;
	}

	public void setCost(float cost) {
		this.cost = cost;
	}
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
