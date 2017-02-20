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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MainDriver {
	
	private static final String ERR_TAG = "inputpath outputpath #_sample[m] #_medoids type[0:40+2k, 1:100+5k]";
	private static final String INIT_MEDOIDS_FOLDER = "immediateOutput2";
	private static final String INIT_MEDOIDS_PATH = "suboutput2/init_medoids";
	private static String inputPath, outputPath, NumOfBEReducer, NumOfMedoids, Type;
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

		try {
			FileSystem fs = FileSystem.get(conf);
			System.out.println("2]Start BestEffort step.");
			
			long start = System.currentTimeMillis();
			
			//get attrNum
			BufferedReader init_br = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))));
			String line = init_br.readLine();
			String[] rl = line.split(",");
			AttrNum = rl.length;
			
			System.out.println("attrNum : " + AttrNum);
			init_br.close();
			conf.set("AttrNum", ""+AttrNum);
			
			//Set First Job.
			Job firstJob = setFirstJob(conf, fs);
			//start First
			firstJob.waitForCompletion(true);
			
			System.out.println("3]First job is completed."); 		
			
			//Set Second Job.
			Job secondJob = setSecondJob(conf, fs);
			
			//merge output file
			immedateBEProcess(fs);
			
			//start Second
			secondJob.waitForCompletion(true);

			long end = System.currentTimeMillis();
			
			System.out.println("time : " + (end-start) + " ms");

		} catch (Exception e) {
			System.out.println("Job start error! \n"+e.getMessage());
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
		if(args.length < 5)
		{
			System.out.println(ERR_TAG);
			System.exit(1);
		}

		inputPath = args[0];
		outputPath = args[1];
		NumOfBEReducer = args[2];
		NumOfMedoids = args[3];
		Type = args[4];
		
		
		//conf set-up
		conf = new Configuration();
		conf.set("InitPath", INIT_MEDOIDS_PATH);
		conf.set("NumOfBEReducer", NumOfBEReducer);
		conf.set("NumOfMedoids",NumOfMedoids);
		conf.set("Type", Type);
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
		Job bestEffortJob = null;
		
		//directory isExist?
		if(fs.exists(new Path(INIT_MEDOIDS_FOLDER)))
			fs.delete(new Path(INIT_MEDOIDS_FOLDER), true);

		try {
			bestEffortJob = new Job(conf, "FirstJob");
			FileInputFormat.addInputPath(bestEffortJob, new Path(inputPath));
			FileOutputFormat.setOutputPath(bestEffortJob, new Path(INIT_MEDOIDS_FOLDER));
			
			bestEffortJob.setJarByClass(MainDriver.class);
			bestEffortJob.setMapperClass(ClaraFirstMapper.class);
			bestEffortJob.setReducerClass(ClaraFirstReducer.class);
			
			bestEffortJob.setInputFormatClass(TextInputFormat.class);
			bestEffortJob.setOutputFormatClass(TextOutputFormat.class);		
			bestEffortJob.setOutputKeyClass(Text.class);
			bestEffortJob.setOutputValueClass(Text.class);
			bestEffortJob.setNumReduceTasks(Integer.parseInt(NumOfBEReducer));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("BestEffortJob setting error!");
		}
		
		
		return bestEffortJob;
	}
	
	
	/**
	 * 
	 * @param conf Configuration
	 * @return Job
	 * @throws IOException 
	 */
	private static Job setSecondJob(Configuration conf, FileSystem fs) throws IOException
	{
		Job topOffJob = null;
		
		//directory isExist?
		if(fs.exists(new Path(outputPath)))
			fs.delete(new Path(outputPath), true);

		try {
			topOffJob = new Job(conf, "SecondJob");
			FileInputFormat.addInputPath(topOffJob, new Path(inputPath));
			FileOutputFormat.setOutputPath(topOffJob, new Path(outputPath));
			
			topOffJob.setJarByClass(MainDriver.class);
			topOffJob.setMapperClass(ClaraSecondMapper.class);
			topOffJob.setReducerClass(ClaraSecondReducer.class);
			topOffJob.setCombinerClass(ClaraSecondReducer.class);
			topOffJob.setInputFormatClass(TextInputFormat.class);
			topOffJob.setOutputFormatClass(TextOutputFormat.class);		
			topOffJob.setOutputKeyClass(Text.class);
			topOffJob.setOutputValueClass(Text.class);
			topOffJob.setNumReduceTasks(1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("TopOffJob setting error!");
		}

		return topOffJob;
	}	
	
	/**
	 * Intermediate Processing
	 * @throws IOException 
	 */
	private static void immedateBEProcess(FileSystem fs) throws IOException
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
				System.out.println(status[i].getPath().toString());
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
	
	}


}
