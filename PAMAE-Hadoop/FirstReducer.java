package dmlab.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FirstReducer extends Reducer<IntWritable, Text, Text, Text>{

	private ArrayList<FloatPoint> dataSet = new ArrayList<FloatPoint>();
	private int NumOfSample = -1;
	private int NumOfMedoids = -1;
	
	private int AttrNum = -1;
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		NumOfMedoids = Integer.parseInt(context.getConfiguration().get("NumOfMedoids"));	
		NumOfSample = Integer.parseInt(context.getConfiguration().get("sampleSize"));	
		AttrNum = Integer.parseInt(context.getConfiguration().get("AttrNum"));
	}

	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
				
		//data store 
		if(dataSet.size() <= NumOfSample)
		{
			for(Text value : values)
			{	
				String[] toks = value.toString().split(",");
				FloatPoint pt = new FloatPoint(AttrNum,-1);
				for(int j=0; j<toks.length; j++)
				{
					pt.getAttr()[j] = (Float.parseFloat(toks[j]));
				}
				dataSet.add(pt);
				
				if(dataSet.size() == NumOfSample)
					break;
			}
		}
		
		outputKey.set(key.toString());
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		//preCaculate
		float[][] preCalcResult = MedoidsAlgorithm.PreCalculate(dataSet);

		//generate init
		List<FloatPoint> Medoids = MedoidsAlgorithm.chooseInitialMedoids_ver2(dataSet, NumOfMedoids, preCalcResult,0.2f);
		
		//Execute Sampling PAM (CLARA) 
		MedoidsAlgorithm.NormalOriginalPAM(dataSet, Medoids, preCalcResult);
		
		for(FloatPoint pt: Medoids)
		{
			outputValue.set(pt.toString());
			context.write(outputKey, outputValue);
		}
	}
	


	
	
	
}
