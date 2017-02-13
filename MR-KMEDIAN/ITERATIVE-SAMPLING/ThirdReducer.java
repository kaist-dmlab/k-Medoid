package dmlab.main;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Write the points which satisfy the condition, larger than 8logn distance.
 * the file name is rPath. this file can be use the input file of next third mapreduce task.
 * ##assign just one reducer task to aggregate one file.##
 * @author Song
 *
 */
public class ThirdReducer extends Reducer<Text, Text, Text, Text>{

	private String rPath;
	
	private FileSystem fs;
	private BufferedWriter r_bw;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		rPath = context.getConfiguration().get("rPath");


		fs = FileSystem.get(context.getConfiguration());
		if(fs.exists(new Path(rPath)))
			fs.delete(new Path(rPath));
				
		r_bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(rPath))));

		
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context arg2)
			throws IOException, InterruptedException {
		
		for(Text value: values)
		{
			r_bw.write(value.toString()+"\n");
		}

	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		r_bw.close();
	}



}
