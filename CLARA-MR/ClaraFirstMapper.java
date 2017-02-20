package dmlab.main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClaraFirstMapper extends Mapper<LongWritable, Text, Text, Text>{

	private String NumOfMedoids = "";
	private int NumOfReducer = 1;
	private Text OutputKey = new Text();

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
		int rand = (int)(Math.random()*NumOfReducer);
		OutputKey.set(""+rand);
	
		context.write(OutputKey, value);
	}



}
