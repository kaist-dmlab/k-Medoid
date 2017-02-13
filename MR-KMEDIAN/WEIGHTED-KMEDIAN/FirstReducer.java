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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FirstReducer extends Reducer<IntWritable, Text, Text, Text>{

	private String samplePath;
	private List<Point> sampleDataSet;
	private List<Point> partition;
	private String Splitter;
	private int AttrNum;
	
	private Text OutputKey;
	private Text OutputValue;

	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		OutputKey = new Text();
		OutputValue = new Text();
		
		sampleDataSet = new ArrayList<Point>();
		partition = new ArrayList<Point>();
		samplePath = context.getConfiguration().get("samplePath");	
		Splitter = context.getConfiguration().get("splitter");
		AttrNum = Integer.parseInt(context.getConfiguration().get("AttrNum"));
			
		FileSystem fs;
		try {
			//sample dataSet read.
			fs = FileSystem.get(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(samplePath))));
			String line = "";
			
			while((line = br.readLine())!=null)
			{
				String[] toks = line.split(Splitter);
				Point pt = new Point(AttrNum,-1);
				
				for(int i=0; i<toks.length; i++)
				{
					pt.getAttr()[i] = (Float.parseFloat(toks[i]));
				}
				sampleDataSet.add(pt);
			}
	
			br.close();
			
		} catch (IOException e) {
			System.out.println("fail to read H set cache!");
		}
		
	}


	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	
		for(Text value: values)
		{
			//string to point format.
			String[] toks = value.toString().split(Splitter);
			Point pt = new Point(AttrNum,-1);
			for(int i=0; i<toks.length; i++)
			{
				pt.getAttr()[i] = (Float.parseFloat(toks[i]));
			}
			partition.add(pt);
		}
		
		for(Point sample: sampleDataSet)
		{
			float min = Float.MAX_VALUE;
			for(Point pt: partition)
			{
				float distance = EuDistanceCalc.CalculateDistance(sample, pt);
				if(min > distance)
					min = distance;
			}
			
			OutputKey.set(sample.toString());
			OutputValue.set(Float.toString(min));
			context.write(OutputKey, OutputValue);
		}	
		
		
	}


	
}
