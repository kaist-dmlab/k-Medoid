package dmlab.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ClaraFirstReducer extends Reducer<Text, Text, Text, Text>{

	private String NumOfMedoids = "";
	private ArrayList<Point> dataSet = new ArrayList<Point>();
	private ArrayList<Point> sampleDataSet = new ArrayList<Point>();
	private int NumOfSample = -1;
	private int AttrNum = -1;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		NumOfMedoids = context.getConfiguration().get("NumOfMedoids");	
		NumOfSample = 100 + 5*Integer.parseInt(NumOfMedoids);
		AttrNum = Integer.parseInt(context.getConfiguration().get("AttrNum"));

	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		//data store 
		//--generate sample index.
		for(Text value : values)
		{	
			String[] toks = value.toString().split(",");
			Point pt = new Point(AttrNum,-1);
			for(int j=0; j<toks.length; j++)
			{
				pt.getAttr()[j]=(Float.parseFloat(toks[j]));
			}
			dataSet.add(pt);
		}
		
		//get sample index & data sampling
		HashSet<Integer> sampleIndex = new HashSet<Integer>();
		int dataSize = dataSet.size();
		while(sampleIndex.size() != NumOfSample)
		{
			sampleIndex.add((int)(Math.random()*dataSize));
		}
		for(Integer sample : sampleIndex)
		{
			sampleDataSet.add(dataSet.get(sample));
		}
		
		//preCaculate
		float[][] preCalcResult = MedoidsAlgorithm.PreCalculate(sampleDataSet);
		
		//generate initial k medoid
		List<Point> Medoids = MedoidsAlgorithm.chooseInitialMedoids_ver2(sampleDataSet, Integer.parseInt(NumOfMedoids), preCalcResult,0.2f);
		
		//Execute Sampling PAM (CLARA) 
		MedoidsAlgorithm.NormalOriginalPAM(sampleDataSet, Medoids, preCalcResult);
		
		for(Point pt: Medoids)
		{
			context.write(key, new Text(pt.toString()));
		}
	}
	
	
	
}
