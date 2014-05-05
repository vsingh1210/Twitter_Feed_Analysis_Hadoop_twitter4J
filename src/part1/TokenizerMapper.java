package part1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import part1.WordCount.TweetCount;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken(" "));
			String tempstr = word.toString().toLowerCase();
			if(!tempstr.contains("foll:") && !tempstr.contains("frnd:") && !tempstr.contains("rt@"))
				context.write(word, one);
			if(tempstr.contains("#"))
				context.getCounter(TweetCount.TotalHashTags).increment(1L);
		}
		context.getCounter(TweetCount.TotalTweets).increment(1L);
	}
}