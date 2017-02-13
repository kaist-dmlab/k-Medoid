package dmlab.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.dmg.pmml.Euclidean;

/**
 * pass Hi, Si information with all edge distances.
 * commonly, the size of sPath is larger than hPath. So, sPath is input of mapper and hPath is cached for each local disk.
 * output key: point string of S subset, value: 8logn-th distance
 */
public class SecondMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private int AttrNum;
	private String Splitter;
	private double StaticSize;
	
	List<Point> S = new ArrayList<Point>();
	List<Point> H = new ArrayList<Point>();
	
	private Text OutputKey = new Text();
	private Text OutputValue = new Text();

	private String hPath;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		AttrNum = Integer.parseInt(context.getConfiguration().get("AttrNum"));
		Splitter = context.getConfiguration().get("splitter");
		hPath = context.getConfiguration().get("hPath");
		StaticSize = Double.parseDouble(context.getConfiguration().get("StaticSize"));
			
		//read H set
		FileSystem fs;
		try {
			fs = FileSystem.get(context.getConfiguration());
			BufferedReader h_br = new BufferedReader(new InputStreamReader(fs.open(new Path(hPath))));
			String line = "";
			
			while((line = h_br.readLine())!=null)
			{
				String[] toks = line.split(Splitter);
				Point pt = new Point(AttrNum,-1);
				
				for(int i=0; i<toks.length; i++)
				{
					pt.getAttr()[i] = (Float.parseFloat(toks[i]));
				}
				H.add(pt);
			}
	
			h_br.close();
			
		} catch (IOException e) {
			System.out.println("fail to read H set cache!");
		}
		
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Point pt = new Point(AttrNum, -1);
		String[] toks = value.toString().split(Splitter);
		for(int i=0; i<toks.length; i++)
			pt.getAttr()[i] = (Float.parseFloat(toks[i]));
		S.add(pt);
	
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		//pass Si, Hi information with all edge distances.
		//select 8logn th point1
		for(Point eOfS: S)
		{
			List<Float> orderedList = new ArrayList<Float>();
			for(Point eOfH: H)
				orderedList.add(EuDistanceCalc.CalculateDistance(eOfS, eOfH));
			Collections.sort(orderedList);
			
			int position = (orderedList.size()+1)-(int)(8.0*Math.log(StaticSize));
			if(position < 0 || position >= orderedList.size())
			{
				position = 0;
			}
			
			OutputKey.set("1");
			OutputValue.set(eOfS.toString()+"\t"+Float.toString(orderedList.get(position)));
			
			context.write(OutputKey, OutputValue);
			
		}
		
	}
	


}
