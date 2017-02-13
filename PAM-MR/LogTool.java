package dmlab.main;

public final class LogTool {

	public static String addPointLineLog(Point[] pt)
	{
		String result = "";
		
		for(int i=0; i<pt.length; i++)
		{
			for(int j=0; j<pt[i].getAttr().length; j++)
			{
				if(j != pt[i].getAttr().length-1)
					result += pt[i].getAttr()[j] +",";
				else
					result += pt[i].getAttr()[j] +"\n";
			}
		}
	return result;
	}
	
}
