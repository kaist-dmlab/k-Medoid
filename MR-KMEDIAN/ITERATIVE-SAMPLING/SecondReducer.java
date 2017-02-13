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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Aggregate the result of mapper to one file.
 * file name: s_primePath.
 * this file has aggregated information about point string and 8logn-th cost sent by previous mapper.
 * ##assign just one reducer task!.##
 * @author Song
 */
public class SecondReducer extends Reducer<Text, Text, Text, Text>{

	private String s_primePath;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		
		s_primePath = context.getConfiguration().get("s_primePath");

	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		

		FileSystem fs;
		try {
			fs = FileSystem.get(context.getConfiguration());
			if(fs.exists(new Path(s_primePath)))
				fs.delete(new Path(s_primePath));
				
			BufferedWriter s_bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(s_primePath))));

			//key is S, value is k th distance
			for(Text value: values)
				s_bw.write(value.toString()+"\n");
				
			s_bw.close();
		} catch (IOException e) {
			System.out.println("fail to read H set cache!");
		}
	}
}
