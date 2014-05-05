package part3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import part3.KMeans.centroid;

public class KMeansMapper extends Mapper<Object, Text, DoubleWritable, Text>{
	private double centroidOne;
	private double centroidTwo;
	private double centroidThree;
	private Text word = new Text();
	private DoubleWritable centroidKey = new DoubleWritable();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		centroidOne = Double.parseDouble(conf.get("c1"));
		centroidTwo = Double.parseDouble(conf.get("c2"));
		centroidThree = Double.parseDouble(conf.get("c3"));
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken(" "));
			String tempstr = word.toString().toLowerCase();
			if(tempstr.contains("foll:")){
				String[] str=word.toString().split(":");
				if(str.length>0){
					double follCount = Double.parseDouble(str[1]);
					if(follCount>0){
						double d1 = Math.abs(centroidOne - follCount);
						double d2 = Math.abs(centroidTwo - follCount);
						double d3 = Math.abs(centroidThree - follCount);
						word.set(Double.toString(follCount));
						if(d1 < d2 && d1 < d3){
							centroidKey.set(centroidOne);
						}else if(d2 < d1 && d2 < d3){
							centroidKey.set(centroidTwo);
						}else{
							centroidKey.set(centroidThree);
						}
						context.getCounter(centroid.keyDisplay).setValue((long)centroidOne);
						context.write(centroidKey,word);
					}
				}
			}
		}
	}
}