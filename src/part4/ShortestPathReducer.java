package part4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import part4.ShortestPath.MoreIterations;

public class ShortestPathReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		String nodes = "UNMODED";
		Text word = new Text();
		int lowCost = 10009;
		for(Text val:values){
			String[] nodeInfo = val.toString().split(" ");
			if(nodeInfo[0].equalsIgnoreCase("NODES")){
				nodes = null;
				nodes = nodeInfo[1];
			}else if(nodeInfo[0].equalsIgnoreCase("VALUE")){
				int cost = Integer.parseInt(nodeInfo[1]);
				if(lowCost > cost){
					context.getCounter(MoreIterations.convergenceCount).increment(1L);
				}
				lowCost = Math.min(cost, lowCost); 
			}
		}
		word.set(lowCost +" "+nodes);
		try {
			context.write(key, word);
			word.clear();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
