package dmlab.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class SecondMapper extends Mapper<LongWritable, Text, Text, Text>{
	private String initPath = "";
	private ArrayList<FloatPoint> k_Medoids = new ArrayList<FloatPoint>();
	private int AttrNum = -1;
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		AttrNum = Integer.parseInt(context.getConfiguration().get("AttrNum"));
		initPath = context.getConfiguration().get("InitPath");
		//read init medois point.
		
		FileSystem fs;
		try {
			fs = FileSystem.get(context.getConfiguration());
			BufferedReader init_br = new BufferedReader(new InputStreamReader(fs.open(new Path(initPath))));
			String line = "";
			
			while((line = init_br.readLine())!=null)
			{
				String[] part = line.split("\t");
				String[] toks = part[1].split(",");
				FloatPoint pt = new FloatPoint(AttrNum,Integer.parseInt(part[0]));
				
				for(int j=0; j<toks.length; j++)
				{
					pt.getAttr()[j] = (Float.parseFloat(toks[j]));
				}
				
				k_Medoids.add(pt);
			}
	
			init_br.close();
		} catch (IOException e) {
			System.out.println("BestEffort Reducer InitReader error!");
		}
		
	}

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		//parsing
		String[] toks = value.toString().split(",");
		FloatPoint pt = new FloatPoint(AttrNum,-1);
		for(int i=0; i<toks.length; i++)
			pt.getAttr()[i] = (Float.parseFloat(toks[i]));
		
		//calculate distance.
		
		HashMap<Integer, Double> map = new HashMap<Integer,Double>();

		for(int i=0; i<k_Medoids.size(); i++)
		{
			
			double dist = EuDistanceCalc.CalculateDistance(pt, k_Medoids.get(i));
			int label = k_Medoids.get(i).getClassLabel();
			
			if(map.containsKey(label))
			{
				if(map.get(k_Medoids.get(i).getClassLabel()) > dist)
					map.put(label, dist);
			}else
			{
				map.put(label, dist);
			}
		}
		
		Set<Entry<Integer, Double>> entries = map.entrySet();
		for(Entry<Integer, Double> entry : entries)
		{
			outputKey.set(""+entry.getKey());	
			outputValue.set(""+entry.getValue());
			context.write(outputKey, outputValue);
		}
	
	}
		



}
