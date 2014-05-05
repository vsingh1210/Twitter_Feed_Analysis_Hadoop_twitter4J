package part3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import part3.KMeans.centroid;

public class KMeansReducer 
extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
	
	private Text result = new Text();
	private double centroidOne;
	private double centroidTwo;
	private double centroidThree;
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		centroidOne = Double.parseDouble(conf.get("c1"));
		centroidTwo = Double.parseDouble(conf.get("c2"));
		centroidThree = Double.parseDouble(conf.get("c3"));
		double sum = 0;
		int count = 0;
		for (Text val : values) {
			sum += Double.parseDouble(val.toString());
			count++;
		}
		result.set(Integer.toString(count));
		double newCentroid = (double) (sum/count);
		if(newCentroid != key.get()){
			context.getCounter(centroid.centroidValue).increment(1L);
			if(key.get()==centroidOne){
				conf.set("c1", Double.toString(newCentroid));
				context.getCounter(centroid.centroidOneVal).setValue((long)newCentroid);
			}
			if(key.get()==centroidTwo){
				conf.set("c2", Double.toString(newCentroid));
				context.getCounter(centroid.centroidTwoVal).setValue((long)newCentroid);
			}
			if(key.get()==centroidThree){
				conf.set("c3", Double.toString(newCentroid));
				context.getCounter(centroid.centroidThreeVal).setValue((long)newCentroid);
			}
			key.set(newCentroid);
		}
		context.write(key, result);
	}
}
