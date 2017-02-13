package dmlab.main;
/**
 * Copyright : Hwanjun Song. (KAIST, Knowledge Service Engineering Department)
 */
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
	
	private static final String ERR_TAG = "inputpath outputpath samplesize #_reducer #_medoids #_core";
	private static final String INIT_MEDOIDS_FOLDER = "firstOutput";
	private static final String INIT_MEDOIDS_PATH = "suboutput/init_medoids";
	private static final String FINAL_INIT_MEDOIDS = null;
	private static String CUSTOM_INIT_PATH = "";
	private static String inputPath, outputPath, sampleSize, NumOfBEReducer, NumOfMedoids,NumOfCores;
	private static String initMedoidsString = "";
	private static String finalMedoidsString = "";
	private static ArrayList<String> beIterationString = new ArrayList<String>();
	private static ArrayList<Double> beIterationCost = new ArrayList<Double>();
	private static double finalCost = 0;
	private static int AttrNum = 0;
	
	public static void main(String[] args)
	{
		//Initialization & Get configuration Object.

		Configuration conf = initDriver(args);
		ArrayList<String> medoidsSet = new ArrayList<String>();
		
		try {
			FileSystem fs = FileSystem.get(conf);
			System.out.println("2]Start.");
			
			long start = System.currentTimeMillis();
			
			//get attrNum
			//BufferedReader init_br = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))));
			//String line = init_br.readLine();
			//String[] rl = line.split(",");
			AttrNum = 13;//rl.length;
			
			System.out.println("attrNum : " + AttrNum);
			//init_br.close();
			conf.set("AttrNum", ""+AttrNum);
			
			
			//Set First Job.
			Job firstJob = setFirstJob(conf, fs);
			//start First
			firstJob.waitForCompletion(true);
			
			System.out.println("3]First job is completed."); 		
			
			//Set Second Job.
			Job secondJob = setSecondJob(conf, fs);
			
			//merge output file
			medoidsSet = immedateBEProcess(fs);
			
			//start Second
			secondJob.waitForCompletion(true);

			System.out.println("------------------------");
			
			//smallest value select.
			int optimalIndex = getOptimalIndex(fs,conf);
			System.out.println("optimal index : " + optimalIndex);
			
			long immediate = System.currentTimeMillis();
			System.out.println("phase 1 time : " + (immediate-start) + " ms");
			
			//final medoids candidate to configuration
			String finalCandidate = "";
			for(int i=0; i<medoidsSet.size(); i++)
			{
				String[] toks = medoidsSet.get(i).split("\t");
				if(Integer.parseInt(toks[0].trim())==optimalIndex)
					finalCandidate += toks[1]+"\n";
			}
			conf.set("FinalCandidate", finalCandidate);
			
			System.out.println(finalCandidate);
			System.out.println("------------------------");
			
			
			//start Third
			//Set Second Job.
			Job thirdJob = setThirdJob(conf, fs);
			//start Second
			thirdJob.waitForCompletion(true);
			
			
			
			long end = System.currentTimeMillis();
			
			System.out.println("phase 1 + 2 time : " + (end-start) + " ms");

			
			immedateTOProcess(fs);
			
		} catch (Exception e) {
			System.out.println("Job start error! \n"+e.toString());
		}
	}

	private static int getOptimalIndex(FileSystem fs, Configuration conf)
	{
		ArrayList<costInfo> infos = new ArrayList<costInfo>();
		
		try {
			BufferedReader init_br = new BufferedReader(new InputStreamReader(fs.open(new Path(outputPath+"/part-r-00000"))));
			String line = "";
			
			
			while((line = init_br.readLine())!=null)
			{
				String[] part = line.split("\t");
				
				costInfo temp = new costInfo();
				temp.key = Integer.parseInt(part[0]);
				temp.cost = Double.parseDouble(part[1]);
				
				infos.add(temp);
			}
	
			init_br.close();
		} catch (IOException e) {
			System.out.println("Reducer InitReader error!");
		}
		
		double min = Double.MAX_VALUE;
		int index = -1;
		for(int i=0; i<infos.size(); i++)
		{
			if(min > infos.get(i).cost)
			{
				min = infos.get(i).cost;
				index = infos.get(i).key;
			}
		}
		System.out.println("min Cost : " + min);
		
		return index;
	}
	
	private static class costInfo{
		int key;
		double cost;
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
		if(args.length != 6)
		{
			System.out.println(ERR_TAG);
			System.exit(1);
		}

		inputPath = args[0];
		outputPath = args[1];
		sampleSize = args[2];
		NumOfBEReducer = args[3];
		NumOfMedoids = args[4];
		NumOfCores = args[5];
		
		
		//conf set-up
		conf = new Configuration();
		conf.set("InitPath", INIT_MEDOIDS_PATH);
		conf.set("NumOfBEReducer", NumOfBEReducer);
		conf.set("NumOfMedoids",NumOfMedoids);
		conf.set("sampleSize", sampleSize);
		conf.set("NumOfCores",NumOfCores);
		conf.set("mapred.task.timeout","5000000");
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
		Job job = null;
		
		//directory isExist?
		if(fs.exists(new Path(INIT_MEDOIDS_FOLDER)))
			fs.delete(new Path(INIT_MEDOIDS_FOLDER), true);

		try {
			job = new Job(conf, "FirstJob");
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(INIT_MEDOIDS_FOLDER));
			
			job.setJarByClass(MainDriver.class);
			job.setMapperClass(FirstMapper.class);
			job.setReducerClass(FirstReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setMapOutputKeyClass(IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(Integer.parseInt(NumOfBEReducer));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("setting error!");
		}
		
		
		return job;
	}
	
	
	/**
	 * 
	 * @param conf Configuration
	 * @return Job
	 * @throws IOException 
	 */
	private static Job setSecondJob(Configuration conf, FileSystem fs) throws IOException
	{
		Job job = null;
		
		//directory isExist?
		if(fs.exists(new Path(outputPath)))
			fs.delete(new Path(outputPath), true);

		try {
			job = new Job(conf, "SecondJob");
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setJarByClass(MainDriver.class);
			job.setMapperClass(SecondMapper.class);
			job.setReducerClass(SecondReducer.class);
			job.setCombinerClass(SecondReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);		
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("setting error!");
		}

		return job;
	}	
	
	private static Job setThirdJob(Configuration conf, FileSystem fs) throws IOException
	{
		Job job = null;
		
		//directory isExist?
		if(fs.exists(new Path(outputPath)))
			fs.delete(new Path(outputPath), true);

		try {
			job = new Job(conf, "ThirdJob");
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setJarByClass(MainDriver.class);
			job.setMapperClass(ThirdMapper.class);
			job.setReducerClass(ThirdReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);		
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(Integer.parseInt(NumOfCores));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("setting error!");
		}

		return job;
	}	
	/**
	 * Immediate Processing
	 * @throws IOException 
	 */
	private static ArrayList<String> immedateBEProcess(FileSystem fs) throws IOException
	{
		System.out.println("3]merge process start."); 
		//0. init medoids load.
		
		
		//1. delete original file.
		fs.delete(new Path(INIT_MEDOIDS_PATH), true); // delete file, true for recursive 
		
		//취합 => string 으로 만들기.
		FileStatus[] status = fs.listStatus(new Path(INIT_MEDOIDS_FOLDER));


		//flitering output files.
		System.out.println("3]Merge BE output.");
		
		ArrayList<String> candidate = new ArrayList<String>();
		double min = Double.MAX_VALUE;
		Path minPath = null;
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
					candidate.add(line);
				}
			}
		}
		
		
		System.out.println("Merge step is complete.");

		
		//3. write new medois
		System.out.println("----------------------------------------------"); 
		
		BufferedWriter init_bw=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(INIT_MEDOIDS_PATH),true)));
		
		for(int i=0; i<candidate.size(); i++)
		{
			if(i!=candidate.size()-1)
				init_bw.write(candidate.get(i)+"\n");
			else
				init_bw.write(candidate.get(i));
		}
		
		init_bw.close();
	
		return candidate;
	}

	/**
	 * Immediate Processing
	 * @throws IOException 
	 */
	private static void immedateTOProcess(FileSystem fs) throws IOException
	{
		System.out.println("----------------------------------------------"); 

		System.out.println("3]merge process start."); 
		//0. init medoids load.

		mergeObject[] temp = new mergeObject[Integer.parseInt(NumOfMedoids)];
		for(int i=0; i<temp.length; i++)
			temp[i] = new mergeObject("", Float.MAX_VALUE);
		
		
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
				int index = 0;
				while((line = br.readLine())!=null && index<Integer.parseInt(NumOfMedoids))
				{
					
					//System.out.println(index);
					String[] sub = line.split("\t");
					temp[index].modify(sub[0], Float.parseFloat(sub[1]));
					index++;
					
				}
			}
		}
		
		System.out.println("Merge step is complete.");
	
		
		//3. write new medois
		for(int i=0; i<temp.length; i++)
			System.out.println(temp[i].point);
			

	
		System.out.println("----------------------------------------------"); 

	}	
	
	static class mergeObject{
		
		public mergeObject(String point, float cost)
		{
			this.point = point;
			this.cost = cost;
		}
		
		public void modify(String point, float cost)
		{
			if(this.cost > cost)
				this.point = point;
		}
		
		String point;
		float cost;
	}
	
}
