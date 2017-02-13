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
	
	private static final String ERR_TAG = "inputpath outputpath #_reducer #_medoids [initMedoidPath]";
	private static final String INIT_MEDOIDS_PATH = "suboutput/init_medoids";
	private static String CUSTOM_INIT_PATH = "";
	private static String inputPath, outputPath, NumOfBEReducer, NumOfMedoids;
	private static ArrayList<Point> dataSet = new ArrayList<Point>();
	private static Point[] newPamInit = null;
	private static Point[] beforePamInit = null;
	//log String variables
	private static String initMedoidsString = "";
	private static String finalMedoidsString = "";
	private static double finalCost = 0;
	private static ArrayList<Double> beIterationCost = new ArrayList<Double>();

	
	public static void main(String[] args)
	{
		//Initialization & Get configuration Object.

		Configuration conf = initDriver(args);
		
		initMedoidsString = LogTool.addPointLineLog(newPamInit);
		
		try {
			FileSystem fs = FileSystem.get(conf);		
			long start = System.currentTimeMillis();
			
			System.out.println("1]Start Fames-Pam MR.");
			
			for(int i=0; i<6; i++)
			{
				long iterStart = System.currentTimeMillis();
				
				//job execution
				Job job = setJob(conf, fs);
				job.waitForCompletion(true);
				
				//integrate.
				immedateTOProcess(fs);
				
				long iterEnd = System.currentTimeMillis();
				System.out.println("1 iteration time : " + (iterEnd-iterStart));
				
			}
			long end = System.currentTimeMillis();	
			System.out.println("execution time : " + (end-start));
		
			summary();

		} catch (Exception e) {
			System.out.println("Job start error! \n"+e.getMessage());
		}
	}

	
	/**
	 * Result phase
	 */
	private static void summary()
	{
		System.out.println("----------------------------------------------"); 
		System.out.println("output summary."); 
		System.out.println("[1]init Point");
		System.out.println(initMedoidsString);
		System.out.println("[4]final medoids");
		System.out.println(finalMedoidsString);

		System.out.println("----------------------------------------------"); 			
		
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
		if(args.length < 4)
		{
			System.out.println(ERR_TAG);
			System.exit(1);
		}

		inputPath = args[0];
		outputPath = args[1];
		NumOfBEReducer = args[2];
		NumOfMedoids = args[3];
		
		if(args.length >4)
		{
			CUSTOM_INIT_PATH = args[4];
			System.out.println("custum init path : " + args[4]);
		}
		
		//conf set-up
		conf = new Configuration();
		conf.set("InitPath", INIT_MEDOIDS_PATH);
		conf.set("NumOfBEReducer", NumOfBEReducer);
		conf.set("NumOfMedoids",NumOfMedoids);
		conf.set("mapred.task.timeout","3600000");
		conf.set("mapred.child.java.opts", "-Xmx25g -Xss1024m");
       
		//make first random K-Medoids.

		FileSystem fs;
		
		try {
			fs = FileSystem.get(conf);
			BufferedReader init_br = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))));
			String line = "";
		
			
			//store dataSet
			int num = 0;
			while((line = init_br.readLine())!=null)
			{
				String[] toks = line.split(",");
				Point pt = new Point(toks.length,-1);
				for(int i=0; i<toks.length; i++)
					pt.getAttr()[i] = (Float.parseFloat(toks[i]));
				dataSet.add(pt);
				num++;
				
				if(num > 10000)
					break;
				
			}
			
			
			
			//conf setting.
			String attrSize = ""+dataSet.get(0).getAttr().length;
			conf.set("AttrSize", attrSize);
			
			init_br.close();
		
			
	    	System.out.println("----------------------------------------------"); 
	    	System.out.println("1] Initial Point Step");

			
			
			String initMedoidsString = "";

			if(CUSTOM_INIT_PATH.compareTo("")!=0)
			{
				newPamInit = new Point[Integer.parseInt(NumOfMedoids)];
				//Custom File Read.
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(CUSTOM_INIT_PATH))));
				String init_line = "";
				for(int i=0; i<Integer.parseInt(NumOfMedoids); i++)
				{
					init_line = br.readLine();
					String[] toks = init_line.split(",");
					Point pt = new Point(toks.length,i);
					for(int j=0; j<toks.length; j++)
						pt.getAttr()[j] = (Float.parseFloat(toks[j]));
					newPamInit[i] = pt;
				}
			}else
			{
				//Random Initioal Point setting
				newPamInit = chooseInitialMedoids(dataSet, Integer.parseInt(NumOfMedoids));
			}
			

			System.out.println("Set up completion.");
			for(int i=0; i<newPamInit.length; i++)
			{
				System.out.println("points("+newPamInit[i].toString()+",,col = 2)");
			}
			System.out.println("----------------------------------------------");  
			
			
			
			//쓸 파일 String 형성.
			for(int i=0; i<newPamInit.length; i++)
			{
					
				if(i != newPamInit.length-1)
				{
					initMedoidsString += newPamInit[i].getClassLabel()+",";
					for(int j=0; j<Integer.parseInt(attrSize); j++)
					{
						if(j!=Integer.parseInt(attrSize)-1)
							initMedoidsString += newPamInit[i].getAttr()[j] +",";
						else
							initMedoidsString += newPamInit[i].getAttr()[j] +"\n";
					}
				}else
				{
					initMedoidsString += newPamInit[i].getClassLabel()+",";
					for(int j=0; j<Integer.parseInt(attrSize); j++)
					{
						if(j!=Integer.parseInt(attrSize)-1)
							initMedoidsString += newPamInit[i].getAttr()[j] +",";
						else
							initMedoidsString += newPamInit[i].getAttr()[j];
					}
				}
			}
			
			

			
			//write init_Medoids file
			BufferedWriter init_bw=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(INIT_MEDOIDS_PATH),true)));
			init_bw.write(initMedoidsString);
			init_bw.close();
		} catch (IOException e) {
			System.out.println("initDriver IOException error!");
		}
		
		return conf;
	}
	
	
	/**
	 * 
	 * @param conf Configuration
	 * @return Job
	 * @throws IOException 
	 */
	private static Job setJob(Configuration conf, FileSystem fs) throws IOException
	{
		Job topOffJob = null;
		
		//directory isExist?
		if(fs.exists(new Path(outputPath)))
			fs.delete(new Path(outputPath), true);

		try {
			topOffJob = new Job(conf, "TopOffJob");
			FileInputFormat.addInputPath(topOffJob, new Path(inputPath));
			FileOutputFormat.setOutputPath(topOffJob, new Path(outputPath));
			
			topOffJob.setJarByClass(MainDriver.class);
			topOffJob.setMapperClass(FamesMapper.class);
			topOffJob.setReducerClass(FamesReducer.class);
			
			topOffJob.setInputFormatClass(TextInputFormat.class);
			topOffJob.setOutputFormatClass(TextOutputFormat.class);		
			topOffJob.setOutputKeyClass(Text.class);
			topOffJob.setOutputValueClass(Text.class);
			topOffJob.setNumReduceTasks(Integer.parseInt(NumOfMedoids));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("TopOffJob setting error!");
		}

		return topOffJob;
	}	
	
	/**
	 * Immediate file processing
	 * @throws IOException 
	 */
	private static void immedateTOProcess(FileSystem fs) throws IOException
	{
		System.out.println("3]merge process start."); 
		//0. init medoids load.
		
		
		//1. delete original file.
		fs.delete(new Path(INIT_MEDOIDS_PATH), true); // delete file, true for recursive 
		//취합 => string 으로 만들기.
		FileStatus[] status = fs.listStatus(new Path(outputPath));


		//flitering output files.
		System.out.println("3]Merge TO output.");
		
		Point[] newMedoids = new Point[Integer.parseInt(NumOfMedoids)];
		
		String finalOutput = "";
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
					String[] attr = line.split("\t");
					System.out.println("points("+attr[1]+",,col = 2)");
					
					String[] ele = attr[1].split(",");
					Point pt = new Point(ele.length,Integer.parseInt(attr[0]));
					for(int j=0; j<ele.length; j++)
						pt.getAttr()[j] = (Float.parseFloat(ele[j]));		
					newMedoids[Integer.parseInt(attr[0])] = pt;
					
					finalOutput += attr[0]+","+attr[1]+"\n";
				}
			}
		}
		
		System.out.println("Merge step is complete.");
		

		//3. write new medois
		BufferedWriter init_bw=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(INIT_MEDOIDS_PATH),true)));
		init_bw.write(finalOutput);
		init_bw.close();
		
		System.out.println(finalOutput);

		//calc cost.

		beforePamInit = newMedoids;
		finalMedoidsString = finalOutput;

		
	}	
	
	/**
	 * chooseInitialMedoids
	 * @param patternList List
	 * @param numOfCluster int
	 * @return Point[]
	 */
    private static Point[] chooseInitialMedoids(List<Point> patternList,int numOfCluster) {
    	

        Point[] medoids = new Point[numOfCluster];
        
        //random k index search.
        HashSet<Integer> initIndex = new HashSet<Integer>();
        while(initIndex.size() != numOfCluster)
        {
        	initIndex.add((int)(Math.random()*patternList.size()));
        }
        
        //insert medoids <= init medoids
        Iterator<Integer> it = initIndex.iterator();
        int iterationIndex = 0;
		while(it.hasNext())
		{
			 Point medoidPattern = patternList.get(it.next());
			 //class label : start to 1
			 medoidPattern.setClassLabel(iterationIndex);
			 medoids[iterationIndex++] = medoidPattern;
		}
		

        return medoids;

}	

}
