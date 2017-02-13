package dmlab.main;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


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
