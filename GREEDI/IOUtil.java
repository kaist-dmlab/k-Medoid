package dmlab.main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class IOUtil {
	public static List<Point> readFile(String inputFile) throws IOException
	{
		FileReader fr = new FileReader(inputFile);
		BufferedReader br = new BufferedReader(fr);
		
		List<Point> dataSet = new ArrayList<Point>();
		
		String line = "";
		while((line=br.readLine())!=null)
		{
			String[] toks = line.toString().split(",");
			Point pt = new Point(toks.length,-1);
			for(int j=0; j<toks.length; j++)
			{
				pt.getAttr()[j] = (Float.parseFloat(toks[j]));
			}
			dataSet.add(pt);
		}
		
		fr.close();
		br.close();
		return dataSet;
	}
}
