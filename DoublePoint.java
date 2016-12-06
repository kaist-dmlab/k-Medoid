package dmlab.main;

import java.io.Serializable;
import java.util.ArrayList;

public class DoublePoint implements Serializable{
	private double[] values = null;
	private int classLabel = -1;
	private int key = -1;
	private int dimension = -1;
	private float cost = -1;
	private double weight = 1;
	
	public DoublePoint(){}
	
	public DoublePoint(int dimension, int classLable)
	{
		this.values = new double[dimension];
		this.classLabel = classLable;
	}
	
	public double getWeight() {return weight;}
	
	public void setWeight(double weight) {this.weight = weight;}
	
	public int getDimension() {return dimension;}

	public void setDimension(int dimension) {this.dimension = dimension;}

	public int getKey() {return key;}

	public void setKey(int key) {this.key = key;}
	
	public float getCost() {return cost;}

	public void setCost(float cost) {this.cost = cost;}

	public double[] getValues() {return values;
	}
	public int getClassLabel() {return classLabel;}

	public void setClassLabel(int classLabel) {this.classLabel = classLabel;}

	public void setvalues(double[] values) {this.values = values;}
	
	public void copyPoint(DoublePoint point)
	{
		this.classLabel = point.classLabel;
		for(int i=0; i<this.values.length; i++)
		{	double temp = point.getValues()[i];
			this.values[i] = (temp);}
	}
	
	public boolean isEqual(DoublePoint other)
	{
		int count = 0;
		for(int i=0; i<this.values.length; i++)
		{	double[] temp = other.getValues();
			if(this.values[i] == temp[i])
			{count++;}}
		if(count == this.values.length) return true;
		else return false;
	}
	
	public String toString()
	{
		String result = "";
		for(int i=0; i<values.length; i++)
		{	if(i != values.length -1) result += values[i] + ",";
			else result += values[i];}
		return result;
	}
	
    public static DoublePoint multiply(DoublePoint a, double k) {
        DoublePoint res = new DoublePoint(a.values.length,-1);
        res.add(a);
        res.multiply(k);
        return res;
    }

    public DoublePoint add(DoublePoint other) {
        for (int i = 0; i < values.length; i++) {
        	values[i] += other.values[i];
        }
        return this;
    }

    public DoublePoint deduce(DoublePoint other) {
        for (int i = 0; i < values.length; i++) {
        	values[i] -= other.values[i];
        }
        return this;
    }

    public static DoublePoint substraction(DoublePoint a, DoublePoint b) {
        DoublePoint res = new DoublePoint(a.values.length,-1);
        for (int i = 0; i < a.values.length; i++) {
            res.values[i] = a.values[i] - b.values[i];
        }
        return res;
    }

    public DoublePoint multiply(double k) {
        for (int i = 0; i < values.length; i++) {
        	values[i] *= k;
        }
        return this;
    }

    public double getNormToThe2() {
        double res = 0;
        for (int i = 0; i < values.length; i++) {
            res += values[i]*values[i];
        }
        return res;
    }

    // euclidean distance as norm
    public double getNorm() {
        return Math.sqrt(getNormToThe2());
    }

    public int compareTo(DoublePoint point) {
        for (int i = 0; i < values.length; i++) {
            if (values[i] != point.values[i]) {
                double dif = values[i] - point.values[i];
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
