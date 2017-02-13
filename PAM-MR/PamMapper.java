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

public class PamMapper extends Mapper<LongWritable, Text, Text, Text>{
	private String initPath = "";
	private String NumOfMedoids = "";
	private int attrSize = 0;
	private ArrayList<Point> k_Medoids = new ArrayList<Point>();
	
	private Text outputKey = new Text();
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		NumOfMedoids = context.getConfiguration().get("NumOfMedoids");	
		initPath = context.getConfiguration().get("InitPath");
		attrSize = Integer.parseInt(context.getConfiguration().get("AttrSize"));
		//read init medois point.
		
		FileSystem fs;
		try {
			fs = FileSystem.get(context.getConfiguration());
			BufferedReader init_br = new BufferedReader(new InputStreamReader(fs.open(new Path(initPath))));
			String line = "";
			//we select top k points to init_medoids.
			for(int i=0; i<Integer.parseInt(NumOfMedoids); i++)
			{
				line = init_br.readLine();
				String[] toks = line.split(",");
				Point pt = new Point(attrSize,Integer.parseInt(toks[0]));
				
				for(int j=1; j<toks.length; j++)
				{
					pt.getAttr()[j-1] = (Float.parseFloat(toks[j]));
				}
				k_Medoids.add(pt);
			}
			init_br.close();
		} catch (IOException e) {
		}
		
	}

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		//parsing
		String[] toks = value.toString().split(",");
		Point pt = new Point(attrSize,-1);
		
	
		
		for(int i=0; i<toks.length; i++)
			pt.getAttr()[i] = (Float.parseFloat(toks[i]));
		
		//assing procedure.
	
		double min = Double.MAX_VALUE;
		int classLabel = -1;
		for(int i=0; i<k_Medoids.size(); i++)
		{
			double distance = EuDistanceCalc.CalculateDistance(k_Medoids.get(i),pt);
			if(distance < min)
			{
				classLabel = k_Medoids.get(i).getClassLabel();
				min = distance;
			}
		}
		//write
			outputKey.set(""+classLabel);
			context.write(outputKey, value);
		
	
	}
		



}
