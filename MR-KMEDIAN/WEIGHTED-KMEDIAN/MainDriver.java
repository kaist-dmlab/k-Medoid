package dmlab.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainDriver {

	//sPath hPath s_primePath rPath
	private static final String ERR_TAG = "inputpath samplepath outputpath NumOfMedoids CoreSize";
	private static final String Splitter = ",";
	private static final String mergePath = "suboutput/mergePath";
	private static final String finalPath = "suboutput/finalPath";
	private static String inputPath, samplePath, outputPath, NumOfMedoids, CoreSize;
	private static int AttrNum;
	
	public static void main(String[] args)
	{
		long start,end;
		Configuration conf = initDriver(args);
		try {
			System.out.println("Weighted K-Median Start.");
			start = System.currentTimeMillis();
			
			FileSystem fs = FileSystem.get(conf);	
			//get attrNum --------------------------------------------------
			BufferedReader init_br = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))));
			String line = init_br.readLine();
			String[] rl = line.split(",");
			AttrNum = rl.length;
			
			System.out.println("attrNum : " + AttrNum);
			init_br.close();

			conf.set("AttrNum", ""+AttrNum);
			//-----------------------------------------------------------------
			
			//First MapReduce
			Job firstJob = setFirstJob(conf, fs);
			firstJob.waitForCompletion(true);
			mergeProcess(fs,mergePath);
			
			//Second MapReduce
			Job secondJob = setSecondJob(conf, fs);
			secondJob.waitForCompletion(true);
			mergeProcess(fs,finalPath);
			
			//read finalOutput
			List<Point> dataSet = new ArrayList<Point>();
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(finalPath))));
			while((line=br.readLine())!=null)
			{
				String[] parts = line.split("\t");
				
				String[] toks = parts[0].split(Splitter);
				Point pt = new Point(AttrNum,-1);
				
				for(int i=0; i<toks.length; i++)
					pt.getAttr()[i] = (Float.parseFloat(toks[i]));
				
				pt.setWeight(Float.parseFloat(parts[1]));
				
				dataSet.add(pt);
			}
			
			//precalculation
			float[][] preCalculation = MedoidsAlgorithm.PreCalculate(dataSet);

			//weighted k median clustering.
			List<Point> seed = MedoidsAlgorithm.chooseInitialMedoids_ver2(dataSet, Integer.parseInt(NumOfMedoids), preCalculation, 0.2f);
			MedoidsAlgorithm.NormalOriginalPAM(dataSet, seed, preCalculation);
			
			end = System.currentTimeMillis();
			System.out.println("time: "+(end-start)/1000.0 +" s");
			
			for(Point pt: seed)
			{
				System.out.println(pt.toString());
			}
			
			
			
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
		if(args.length != 5)
		{
			System.out.println(ERR_TAG);
			System.exit(1);
		}
		inputPath = args[0];
		samplePath = args[1];
		outputPath = args[2];
		NumOfMedoids = args[3];
		CoreSize = args[4];
		
		//conf set-up
		conf = new Configuration();
		conf.set("NumOfMedoids",NumOfMedoids);
		conf.set("splitter", Splitter);
		conf.set("samplePath", samplePath);
		conf.set("CoreSize", CoreSize);
		conf.set("mapred.task.timeout","5000000");
		conf.set("mapred.child.java.opts", "-Xmx20g -Xss1024m");
		
		return conf;
	}
	
	private static Job setFirstJob(Configuration conf, FileSystem fs) throws IOException
	{
		Job job = null;
		fs.delete(new Path(outputPath), true); // delete file, true for recursive 
				
		try {
			job = new Job(conf, "FirstJob");
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setJarByClass(MainDriver.class);
			job.setMapperClass(FirstMapper.class);
			job.setReducerClass(FirstReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(Integer.parseInt(CoreSize));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("First Job setting error!");
		}
		
		return job;
	}

	private static void mergeProcess(FileSystem fs, String path) throws IOException
	{
		System.out.println("3]merge process start."); 
		//취합 => string 으로 만들기.
		FileStatus[] status = fs.listStatus(new Path(outputPath));

		//flitering output files.
		System.out.println("3]Merge first mapreduce output.");
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path),true)));
		for(int i=0; i<status.length; i++)
		{
			String[] toks = status[i].getPath().toString().split("/");

			if(toks[toks.length-1].length()>6 && toks[toks.length-1].substring(0, 6).compareTo("part-r")==0)
			{
				//file read.
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line = "";
				while((line = br.readLine())!=null)
					bw.write(line+"\n");
				br.close();
			}
		}			
		bw.close();

		System.out.println("Merge step is complete.");

	}
	
	private static Job setSecondJob(Configuration conf, FileSystem fs) throws IOException
	{
		Job job = null;
		fs.delete(new Path(outputPath), true); // delete file, true for recursive 
	
		try {
			job = new Job(conf, "SecondJob");
			FileInputFormat.addInputPath(job, new Path(mergePath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setJarByClass(MainDriver.class);
			job.setMapperClass(SecondMapper.class);
			job.setReducerClass(SecondReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setMapOutputKeyClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("First Job setting error!");
		}
		
		return job;
	}
}
