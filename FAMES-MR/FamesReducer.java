package dmlab.main;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FamesReducer extends Reducer<Text, Text, Text, Text>{
	private int attrSize = 0;
	private String NumOfMedoids = "";
	private Text outputText = new Text();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		NumOfMedoids = context.getConfiguration().get("NumOfMedoids");	
		attrSize = Integer.parseInt(context.getConfiguration().get("AttrSize"));
	}
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{

		Point newMedoid;

		//1. cluster points object generate.
		ArrayList<Point> eachClusterPoints = new ArrayList<Point>();
		for(Text value : values)
		{
			String[] toks = value.toString().split(",");
			Point pt = new Point(attrSize,Integer.parseInt(key.toString()));
			
			for(int i=0; i<attrSize; i++)
			{
				pt.getAttr()[i] = (Float.parseFloat(toks[i]));
			}
			
			eachClusterPoints.add(pt);
		}
		
		//2. new centroid calculate.
		newMedoid = FAMES.doFAMES(eachClusterPoints);

		String output = "";
		for(int i=0; i<attrSize; i++)
		{
			if(i != attrSize-1)
				output += newMedoid.getAttr()[i]+",";
			else
				output +=newMedoid.getAttr()[i];
		}
		
		outputText.set(output);
		context.write(key, outputText);

	}




}
