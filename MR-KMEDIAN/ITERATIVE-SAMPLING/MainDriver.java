package dmlab.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

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

	private static final String ERR_TAG = "inputpath outputpath NumOfMedoids Upsilon CoreSize DataSize";
	private static final String sPath = "suboutput/sPath";
	private static final String hPath = "suboutput/hPath";
	private static final String s_primePath = "suboutput/s_primePath";
	private static final String rPath = "suboutput/rPath";
	private static final String sTemp = "suboutput/sTemp";
	private static final String hTemp = "suboutput/rTemp";
	private static final String Splitter = ",";
	private static String inputPath, outputPath, NumOfMedoids, Upsilon, CoreSize;
	private static int AttrNum;
	private static double DataSize;
	
	public static void main(String[] args)
	{
		long start,end;
		Configuration conf = initDriver(args);
		try {
			System.out.println("Iterative Sampling Start.");
			start = System.currentTimeMillis();
			
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path("suboutput"), true); // delete file, true for recursive 
			
			
			//get attrNum --------------------------------------------------
			BufferedReader init_br = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))));
			String line = init_br.readLine();
			String[] rl = line.split(",");
			AttrNum = rl.length;
			
			System.out.println("attrNum : " + AttrNum);
			init_br.close();

			conf.set("AttrNum", ""+AttrNum);
			//-----------------------------------------------------------------

			double condition1 = (4.0/Double.parseDouble(Upsilon))*Double.parseDouble(NumOfMedoids)
					*Math.pow(DataSize, Double.parseDouble(Upsilon))*Math.log(DataSize);
		
			System.out.println("While Condition: " + condition1);
			
			while(DataSize > condition1)
			{	
				//First MapReduce
				Job firstJob = setFirstJob(conf, fs);
				firstJob.waitForCompletion(true);
				mergeProcess(fs);
				
				//Second MapReduce
				Job secondJob = setSecondJob(conf, fs);
				secondJob.waitForCompletion(true);
				
				//Third MapReduce
				Job thirdJob = setThirdJob(conf, fs);
				thirdJob.waitForCompletion(true);
				
				//-----------Data Size update--------------
				inputPath = rPath;
				DataSize = countLineOfFile(fs, inputPath);
				
				conf.set("DataSize", ""+DataSize);
				
				System.out.println("updated R size : " + DataSize);
				
			}
			
			System.out.println("");
			end = System.currentTimeMillis();
			System.out.println("time: "+(end-start)/1000.0 +" s");
			System.out.println("");
			
			
			String finalInput = null;
			
			if(fs.exists(new Path(rPath)))
			{
				finalInput = rPath;
				//return S U R, standard output
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(sPath))));
				while((line=br.readLine())!=null)
					System.out.println(line);
				
				br = new BufferedReader(new InputStreamReader(fs.open(new Path(rPath))));
				while((line=br.readLine())!=null)
					System.out.println(line);
			}else{
				finalInput = inputPath;
				//return S U R, standard output
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))));
				while((line=br.readLine())!=null)
					System.out.println(line);
			}
				
			
		} catch (Exception e) {
			System.out.println("Job start error! \n"+e.toString());
		}
	}
	
	private static int countLineOfFile(FileSystem fs, String fileName) throws IllegalArgumentException, IOException
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))));
		String line = null;
		
		int num = 0;
		
		while((line=br.readLine())!=null)
		{
			num++;
		}
		
		return num;
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
		NumOfMedoids = args[2];
		Upsilon = args[3];
		CoreSize = args[4];
		DataSize = Double.parseDouble(args[5]);
		
		System.out.println("Line Size : "+DataSize);
		
		
		//conf set-up
		conf = new Configuration();
		conf.set("NumOfMedoids",NumOfMedoids);
		conf.set("splitter", Splitter);
		conf.set("Upsilon", Upsilon);
		conf.set("sPath", sPath);
		conf.set("hPath", hPath);
		conf.set("s_primePath", s_primePath);
		conf.set("rPath", rPath);
		conf.set("CoreSize", CoreSize);
		
		conf.set("mapred.task.timeout","5000000");
		conf.set("mapred.child.java.opts", "-Xmx20g -Xss1024m");
		
		//data size set for initial dataset.
		conf.set("StaticSize", ""+DataSize);
		conf.set("DataSize", ""+DataSize);
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
	
	/**
	 * 
	 * @param conf Configuration
	 * @return Job
	 * @throws IOException 
	 */
	private static Job setSecondJob(Configuration conf, FileSystem fs) throws IOException
	{
		Job job = null;
		int NumOfReducer = 1;
		fs.delete(new Path(outputPath), true); // delete file, true for recursive 
				
		
		try {
			job = new Job(conf, "SecondJob");
			FileInputFormat.addInputPath(job, new Path(sPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setJarByClass(MainDriver.class);
			job.setMapperClass(SecondMapper.class);
			job.setReducerClass(SecondReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setMapOutputKeyClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(NumOfReducer);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("First Job setting error!");
		}
		
		return job;
	}
	
	/**
	 * 
	 * @param conf Configuration
	 * @return Job
	 * @throws IOException 
	 */
	private static Job setThirdJob(Configuration conf, FileSystem fs) throws IOException
	{
		Job job = null;
		int NumOfReducer = 1;
		fs.delete(new Path(outputPath), true); // delete file, true for recursive 
		
		
		try {
			job = new Job(conf, "ThirdJob");
			
			if(fs.exists(new Path(rPath)))
			{
				FileInputFormat.addInputPath(job, new Path(rPath));
				System.out.println("rPath exist");
			}
			else
			{
				FileInputFormat.addInputPath(job, new Path(inputPath));
				System.out.println("rPath not exist");
			}
				
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setJarByClass(MainDriver.class);
			job.setMapperClass(ThirdMapper.class);
			job.setReducerClass(ThirdReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setMapOutputKeyClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(NumOfReducer);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("First Job setting error!");
		}
		
		return job;
	}
	
	/**
	 * Merge Processing
	 * @throws IOException 
	 */
	private static void mergeProcess(FileSystem fs) throws IOException
	{
		System.out.println("3]merge process start."); 
		fs.delete(new Path(sTemp));
		fs.delete(new Path(hTemp));
		
		
		//취합 => string 으로 만들기.
		FileStatus[] status = fs.listStatus(new Path(outputPath));

		//flitering output files.
		System.out.println("3]Merge first mapreduce output.");
		
		BufferedWriter s_bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(sPath),true)));
		BufferedWriter h_bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(hPath),true)));
		//exist?
		if(fs.exists(new Path(sPath)))
		{
			//기존것 복사
			BufferedWriter s_temp_bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(sTemp),true)));
			BufferedWriter h_temp_bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(hTemp),true)));
			
			String line = null;
			BufferedReader s_br = new BufferedReader(new InputStreamReader(fs.open(new Path(sPath))));
			while((line=s_br.readLine())!=null)
				s_temp_bw.write(line+"\n");
			BufferedReader h_br = new BufferedReader(new InputStreamReader(fs.open(new Path(hPath))));
			while((line=h_br.readLine())!=null)
				h_temp_bw.write(line+"\n");
			
			s_temp_bw.close();
			h_temp_bw.close();			
			
			
		}
		
		for(int i=0; i<status.length; i++)
		{
			String[] toks = status[i].getPath().toString().split("/");

			if(toks[toks.length-1].length()>6 && toks[toks.length-1].substring(0, 6).compareTo("part-r")==0)
			{
				//file read.
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line = "";
				while((line = br.readLine())!=null)
				{
					//[0] S or H, [1] Point
					String[] subtoks = line.split("\t");
					if(subtoks[0].equals("S"))
						s_bw.write(subtoks[1]+"\n");
					else
						h_bw.write(subtoks[1]+"\n");
				}
				br.close();
			}
		}			
		
		//copy
		if(fs.exists(new Path(sTemp)))
		{
			BufferedReader s_temp_br =  new BufferedReader(new InputStreamReader(fs.open(new Path(sTemp))));
			BufferedReader h_temp_br =  new BufferedReader(new InputStreamReader(fs.open(new Path(hTemp))));
			String line = null;
			
			while((line=s_temp_br.readLine())!=null)
				s_bw.write(line+"\n");
			while((line=h_temp_br.readLine())!=null)
				h_bw.write(line+"\n");
			
			s_temp_br.close();
			h_temp_br.close();

		}

		
		s_bw.close();
		h_bw.close();
		
		System.out.println("Merge step is complete.");

	}
}
