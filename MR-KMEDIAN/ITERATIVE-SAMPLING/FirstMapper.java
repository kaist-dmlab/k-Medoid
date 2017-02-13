package dmlab.main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The mappers arbitaraily partition R into [R/n^upsilon] sets of size
 * at most n^upsilon. Each of these sets is mapped to a unique reducer.
 */
public class FirstMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

	private int CoreSize;
	private IntWritable OutputKey = new IntWritable();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		CoreSize = Integer.parseInt(context.getConfiguration().get("CoreSize"));	
	}

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {

		int NumOfReducer = CoreSize;
		int rand = (int)(Math.random()*NumOfReducer);
		OutputKey.set(rand);
		
		context.write(OutputKey, value);
		
	}


}
