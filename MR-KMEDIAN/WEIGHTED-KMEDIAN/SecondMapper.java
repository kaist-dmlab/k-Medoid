package dmlab.main;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SecondMapper extends Mapper<LongWritable, Text, Text, Text>{

	private Text OutputKey = new Text();
	private Text OutputValue = new Text();
	


	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {

		String[] toks = value.toString().split("\t");
		OutputKey.set(toks[0]);
		OutputValue.set(toks[1]);
		
		context.write(OutputKey, OutputValue);
		
	}


}
