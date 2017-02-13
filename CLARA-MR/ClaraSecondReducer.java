package dmlab.main;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClaraSecondReducer extends Reducer<Text, Text, Text, Text>{
	private Text outputText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{

		double cost = 0;
		
		for(Text value : values)
		{
			cost += Double.parseDouble(value.toString());
		}
		
		
		outputText.set(cost+"");
		context.write(key, outputText);

	}




}
