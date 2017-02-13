package dmlab.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
	private ArrayList<Point> dataSet = new ArrayList<Point>();
	private int NumOfMedoids;
	private int AttrNum;
	private Text OutputValue = new Text();
	private String Splitter;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		NumOfMedoids = Integer.parseInt(context.getConfiguration().get("NumOfMedoids"));
		AttrNum = Integer.parseInt(context.getConfiguration().get("AttrNum"));
		Splitter = context.getConfiguration().get("splitter");
	}
	
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
	
		//data store 
		for(Text value : values)
		{	
			String[] toks = value.toString().split(Splitter);
			Point pt = new Point(AttrNum,-1);
			for(int j=0; j<toks.length; j++)
			{
				pt.getAttr()[j] = (Float.parseFloat(toks[j]));
			}
			dataSet.add(pt);
		}	
		//perform standard greedy algorithm
		List<Point> immediateMedoids = GREEDI.GREEDI(dataSet, NumOfMedoids);
		
		for(int i=0; i<immediateMedoids.size(); i++)
		{
			OutputValue.set(immediateMedoids.get(i).toString());
			context.write(key, OutputValue);
		}
	}

}
