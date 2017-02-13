package dmlab.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class ThirdMapper extends Mapper<LongWritable, Text, Text, Text>{
	private ArrayList<FloatPoint> k_Medoids = new ArrayList<FloatPoint>();
	private int AttrNum = -1;
	private int NumOfCores = -1;
	private Text outputKey = new Text();
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		AttrNum = Integer.parseInt(context.getConfiguration().get("AttrNum"));
		NumOfCores = Integer.parseInt(context.getConfiguration().get("NumOfCores"));
	}

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		//partitioning.
		//key random Generate.
		int rand = (int)(Math.random()*NumOfCores);
		outputKey.set(""+rand);

		context.write(outputKey, value);
		
	}
}
