package part4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ShortestPathMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Text word = new Text();
		String gNode = value.toString();
		String[] nodeInfo=gNode.split("\\s+");
		int cost = Integer.parseInt(nodeInfo[1]) + 1;
		String[] adjNodes = nodeInfo[2].split(":");
		for(int i = 0; i < adjNodes.length; i++){
			if(!adjNodes[i].isEmpty()){
				word.set("VALUE " + cost);
				context.write(new LongWritable(Integer.parseInt(adjNodes[i])), word);
				word.clear();
			}
		}
		word.set("VALUE "+ nodeInfo[1]);
		context.write(new LongWritable(Integer.parseInt(nodeInfo[0])), word);
		word.clear();
		word.set("NODES "+ nodeInfo[2]);
		context.write(new LongWritable(Integer.parseInt(nodeInfo[0])), word);
		word.clear();
	}
}