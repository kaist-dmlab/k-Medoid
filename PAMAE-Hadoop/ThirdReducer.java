package dmlab.main;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ThirdReducer extends Reducer<Text, Text, Text, Text>{
	private Text outputKey = new Text();
	private Text outputText = new Text();
	private String candidate = "";
	private String NumOfMedoids = "";
	private ArrayList<DoublePoint> k_Medoids = new ArrayList<DoublePoint>();
	private int AttrNum = -1;
	private ArrayList<DoublePoint> dataSet = new ArrayList<DoublePoint>();
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		k_Medoids.clear();
		AttrNum = Integer.parseInt(context.getConfiguration().get("AttrNum"));
		NumOfMedoids = context.getConfiguration().get("NumOfMedoids");	
		candidate = context.getConfiguration().get("FinalCandidate");	
		//read init medois point.
		
		String[] lines = candidate.split("\n");
		for(int i=0; i<lines.length; i++)
		{
			String[] toks = lines[i].split(",");
			DoublePoint pt = new DoublePoint(AttrNum,i);
			
			for(int j=0; j<toks.length; j++)
			{
				pt.getAttr()[j]=(Float.parseFloat(toks[j]));
			}
			k_Medoids.add(pt);
		}
	}
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		
		//parsing
		dataSet.clear();
		
		for(Text value : values)
		{
			String[] toks = value.toString().split(",");
			DoublePoint pt = new DoublePoint(AttrNum,-1);
			for(int i=0; i<toks.length; i++)
				pt.getAttr()[i] = (Float.parseFloat(toks[i]));	
			dataSet.add(pt);
			
		}
		
		for(DoublePoint pt: k_Medoids)
			dataSet.add(pt);
		


	}


	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		
		if(dataSet.size()>0)
		{
			ArrayList<DoublePoint> finalMedoids = MedoidsAlgorithm.RefineWeiszfeld(dataSet, k_Medoids, 0.01);
			
			//update to real point
	
			for(DoublePoint pt : finalMedoids)
			{
				float min = Float.MAX_VALUE;
				int minLabel = -1;
				
				for(int i=0; i<dataSet.size(); i++)
				{
					float dist = EuDistanceCalc.CalculateDistance(dataSet.get(i), pt);
					if(dist < min)
					{
						min = dist;
						minLabel = i;
					}
				}
					
				pt.setAttr(dataSet.get(minLabel).getAttr());
				
				outputKey.set(pt.toString());
				outputText.set(""+pt.getCost());
				context.write(outputKey, outputText);
			}
			
		}
		
		
		
		
	}




}
