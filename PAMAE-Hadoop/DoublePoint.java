package dmlab.main;

import java.io.Serializable;
import java.util.ArrayList;

public class DoublePoint implements Serializable{
	private double[] attr;
	private int classLabel = -1;
	private int key = -1;
	private int pointNum = -1;
	private double cost = 0;
	private double weight = 1;
	
	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

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
	
	public double getCost() {
		return cost;
	}

	public void setCost(float cost) {
		this.cost = cost;
	}
	public DoublePoint()
	{

	}
	
	public DoublePoint(int size, int classLable)
	{
		attr = new double[size];
		this.classLabel = classLable;
	}

	public double[] getAttr() {
		return attr;
	}

	public int getClassLabel() {
		return classLabel;
	}

	public void setClassLabel(int classLabel) {
		this.classLabel = classLabel;
	}

	public void setAttr(double[] ptrs) {
		this.attr = ptrs;
	}
	
	public void copyPoint(DoublePoint point)
	{
		this.classLabel = point.classLabel;
		
		attr = new double[point.getAttr().length];
		for(int i=0; i<this.attr.length; i++)
		{
			double temp = point.getAttr()[i];
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
	
	public boolean isSamePoint(DoublePoint other)
	{
		int count = 0;
		
		for(int i=0; i<this.attr.length; i++)
		{
			double[] temp = other.getAttr();
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
    
	//new function
    public static DoublePoint multiply(DoublePoint a, double k) {
        DoublePoint res = new DoublePoint(a.attr.length,-1);

        res.add(a);
        res.multiply(k);

        return res;
    }


    
    public DoublePoint add(DoublePoint other) {
        for (int i = 0; i < attr.length; i++) {
        	attr[i] += other.attr[i];
        }
        return this;
    }

    public DoublePoint deduce(DoublePoint other) {
        for (int i = 0; i < attr.length; i++) {
        	attr[i] -= other.attr[i];
        }
        return this;
    }

    public static DoublePoint substraction(DoublePoint a, DoublePoint b) {
        DoublePoint res = new DoublePoint(a.attr.length,-1);
        for (int i = 0; i < a.attr.length; i++) {
            res.attr[i] = a.attr[i] - b.attr[i];
        }
        return res;
    }

    public DoublePoint multiply(double k) {
        for (int i = 0; i < attr.length; i++) {
        	attr[i] *= k;
        }
        return this;
    }

    public double getNormToThe2() {
        double res = 0;
        for (int i = 0; i < attr.length; i++) {
            res += attr[i]*attr[i];
        }
        return res;
    }

    // euclidean distance as norm
    public double getNorm() {
        return Math.sqrt(getNormToThe2());
    }

    public int compareTo(DoublePoint point) {
        for (int i = 0; i < attr.length; i++) {
            if (attr[i] != point.attr[i]) {
                double dif = attr[i] - point.attr[i];
                if (dif > 0d) {
                    return 1;
                } else {
                    return -1;
                }
            }
        }
        return 0;
    } 
}
