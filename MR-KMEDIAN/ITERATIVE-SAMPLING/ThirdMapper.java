package dmlab.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Remove the points from R set if the pair distance between R and S is smaller than 8logn-th distance of each S point.
 * Input is total R dataset, first iteration R is total dataset. 
 * next Iteration R is result of previous mapreduce process.
 * @author Song
 *
 */
public class ThirdMapper extends Mapper<LongWritable, Text, Text, Text>{

	private int AttrNum;
	private String Splitter;
	
	private String s_primePath;
	
	List<Point> s_prime = new ArrayList<Point>();
	
	private Text OutputKey = new Text("R");
	
	int count = 0;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		
		
		AttrNum = Integer.parseInt(context.getConfiguration().get("AttrNum"));
		Splitter = context.getConfiguration().get("splitter");
		s_primePath = context.getConfiguration().get("s_primePath");
	
		
		//read H set
		FileSystem fs;
		
		try {
			fs = FileSystem.get(context.getConfiguration());
			BufferedReader s_br = new BufferedReader(new InputStreamReader(fs.open(new Path(s_primePath))));
			String line = "";
			
			while((line = s_br.readLine())!=null)
			{
				String[] sub = line.split("\t");
				String[] toks = sub[0].split(Splitter);
				Point pt = new Point(AttrNum,-1);
				
				for(int i=0; i<toks.length; i++)
				{
					pt.getAttr()[i] = (Float.parseFloat(toks[i]));
				}
				
				pt.setCost(Float.parseFloat(sub[1]));
				
				s_prime.add(pt);
				
			}
	
			s_br.close();
			
		} catch (IOException e) {
			System.out.println("fail to read H set cache!");
		}
	}
	


	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
		String[] toks = value.toString().split(Splitter);
		Point pt = new Point(AttrNum,-1);
		for(int i=0; i<toks.length; i++)
		{
			pt.getAttr()[i] = (Float.parseFloat(toks[i]));
		}

		boolean exist = true;
		for(int i=0; i<s_prime.size(); i++)
		{
			float distance = EuDistanceCalc.CalculateDistance(s_prime.get(i), pt);
			if(distance < s_prime.get(i).getCost())
			{
				exist = false;
			}
		}
		
		if(exist){
			context.write(OutputKey, value);
		}
			
	}


}
