package dmlab.main;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FirstReducer extends Reducer<IntWritable, Text, Text, Text>{
	private int NumOfMedoids;
	private double DataSize;
	private double StaticSize;
	private double Upsilon;
	
	/**
	 * Sampling S and H subset based on probability.
	 */
	private Text OutputKey = new Text();

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		NumOfMedoids = Integer.parseInt(context.getConfiguration().get("NumOfMedoids"));
		DataSize = Double.parseDouble(context.getConfiguration().get("DataSize"));
		StaticSize = Double.parseDouble(context.getConfiguration().get("StaticSize"));
		Upsilon = Double.parseDouble(context.getConfiguration().get("Upsilon"));
		
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		double probForS = (9.0*NumOfMedoids*Math.pow(StaticSize, Upsilon))/DataSize;
		double probForH = (4.0*Math.pow(StaticSize, Upsilon))/DataSize;	
		
			for(Text value: values)
			{
				if(Math.random() < probForS)
				{
					OutputKey.set("S");
					context.write(OutputKey,value);
				}
				if(Math.random() < probForH)
				{
					OutputKey.set("H");
					context.write(OutputKey,value);
				}
			}
		
	}
	
}
