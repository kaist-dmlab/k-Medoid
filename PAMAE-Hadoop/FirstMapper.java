package dmlab.main;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	private String NumOfMedoids = "";
	private int NumOfReducer = 1;
	private IntWritable OutputKey = new IntWritable();

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	NumOfReducer = Integer.parseInt(context.getConfiguration().get("NumOfBEReducer"));	
	NumOfMedoids = context.getConfiguration().get("NumOfMedoids");	
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		//key random Generate.
		int rand = (int)(Math.random()*Integer.MAX_VALUE);
		OutputKey.set(rand);
	
		context.write(OutputKey, value);
	}



}
