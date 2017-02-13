package dmlab.main;

import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainDriver {
	
	private static final String ERR_TAG = "inputpath outputpath #_reducer #_medoids";
	private static final String INIT_MEDOIDS_PATH = "suboutput/init_medoids";
	private static final String splitter = ",";
	private static String inputPath, outputPath, NumOfReducer, NumOfMedoids;
	
	private static int AttrNum;
	
	public static void main(String[] args)
	{
		long start,end;
		Configuration conf = initDriver(args);
		try {
			System.out.println("Start.");
			start = System.currentTimeMillis();
			
			FileSystem fs = FileSystem.get(conf);
			
			//get attrNum---------------------------------------------------
			BufferedReader init_br = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))));
			String line = init_br.readLine();
			String[] rl = line.split(",");
			AttrNum = rl.length;
			
			System.out.println("attrNum : " + AttrNum);
			init_br.close();
			
			conf.set("AttrNum", ""+AttrNum);
			conf.set("splitter", ",");
			//--------------------------------------------------------------
			
			//First Job: GREEDI based on partitioning
			Job firstJob = setFirstJob(conf, fs);
			firstJob.waitForCompletion(true);
			
			//Merge the result of previous step.
			List<Point> mergeSet = mergeProcess(fs);
			System.out.println("merge size : "+mergeSet.size());
			
			//perform standard greedy algorithm on merge data set.
			List<Point> finalMedoids = GREEDI.GREEDI(mergeSet, Integer.parseInt(NumOfMedoids));
			
			for(int i=0; i<finalMedoids.size(); i++)
				System.out.println(finalMedoids.get(i).toString());
			System.out.println("\n");
			
			end = System.currentTimeMillis();
			System.out.println("time: "+(end-start)/1000.0 +" s");
			
		} catch (Exception e) {
			System.out.println("Job start error! \n"+e.toString());
		}
	}
	
	/**
	  * Driver initialization.
	  * @param args String[] 
	  * @return Configuration
	  */
	private static Configuration initDriver(String[] args)
	{	
		Configuration conf = null;
		
		//init variable.
		if(args.length != 4)
		{
			System.out.println(ERR_TAG);
			System.exit(1);
		}

		inputPath = args[0];
		outputPath = args[1];
		NumOfReducer = args[2];
		NumOfMedoids = args[3];
		
		//conf set-up
		conf = new Configuration();
		conf.set("InitPath", INIT_MEDOIDS_PATH);
		conf.set("NumOfMedoids",NumOfMedoids);
		conf.set("NumOfReducer", NumOfReducer);
		conf.set("mapred.task.timeout","10000000");
		conf.set("mapred.child.java.opts", "-Xmx20g -Xss1024m");
		
		return conf;
	}
	
	/**
	 * 
	 * @param conf Configuration
	 * @return Job
	 * @throws IOException 
	 */
	private static Job setFirstJob(Configuration conf, FileSystem fs) throws IOException
	{
		Job firstJob = null;
		
		//directory isExist?
		if(fs.exists(new Path(outputPath)))
			fs.delete(new Path(outputPath), true);

		
		try {
			firstJob = new Job(conf, "FirstJob");
			FileInputFormat.addInputPath(firstJob, new Path(inputPath));
			FileOutputFormat.setOutputPath(firstJob, new Path(outputPath));
			
			firstJob.setJarByClass(MainDriver.class);
			firstJob.setMapperClass(FirstMapper.class);
			firstJob.setReducerClass(FirstReducer.class);
			
			firstJob.setInputFormatClass(TextInputFormat.class);
			firstJob.setOutputFormatClass(TextOutputFormat.class);
			
			firstJob.setMapOutputKeyClass(IntWritable.class);
			
			firstJob.setOutputKeyClass(IntWritable.class);
			firstJob.setOutputValueClass(Text.class);
			firstJob.setNumReduceTasks(Integer.parseInt(NumOfReducer));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("BestEffortJob setting error!");
		}
		
		return firstJob;
	}

	/**
	 * Merge the result of first step Processing
	 * @throws IOException 
	 */
	private static ArrayList<Point> mergeProcess(FileSystem fs) throws IOException
	{
		System.out.println("3]merge process start."); 
	
		ArrayList<Point> mergeSet = new ArrayList<Point>();
		
		//취합 => string 으로 만들기.
		FileStatus[] status = fs.listStatus(new Path(outputPath));

		//flitering output files.
		System.out.println("3]Merge TO output.");
		
		for(int i=0; i<status.length; i++)
		{
			String[] toks = status[i].getPath().toString().split("/");

			if(toks[toks.length-1].length()>6 && toks[toks.length-1].substring(0, 6).compareTo("part-r")==0)
			{
				//file read.
				//System.out.println(status[i].getPath().toString());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line = "";
				
				while((line = br.readLine())!=null)
				{
					String[] tempToks = line.toString().split("\t");
					String[] subToks = tempToks[1].split(splitter);
					
					Point pt = new Point(subToks.length,-1);
					for(int j=0; j<subToks.length; j++)
					{
						pt.getAttr()[j] = (Float.parseFloat(subToks[j]));
					}
					mergeSet.add(pt);	
				}
			}
		}
		
		return mergeSet;
	}
}
