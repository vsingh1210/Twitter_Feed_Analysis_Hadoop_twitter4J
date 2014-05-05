package part2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripeMapper extends Mapper<LongWritable,Text,Text,MapWritable> {
	  private MapWritable occurrenceMap = new MapWritable();
	  private Text word = new Text();
	  private final static IntWritable one = new IntWritable(1);
	  @Override
	 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	   
		  	StringTokenizer itr = new StringTokenizer(value.toString());
			String pairWord="";
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken(" "));
			String tempstr = word.toString().toLowerCase();
			if(tempstr.charAt(0)!='@' && !tempstr.contains("foll:") && !tempstr.contains("frnd:") && !tempstr.contains("rt@")){
				pairWord+=tempstr.toUpperCase()+":";
			}
		}
		if(pairWord.length()>0){
			pairWord = pairWord.substring(0,pairWord.length() - 1);
			String pairWordArray[]=pairWord.split(":");
				for (int i = 0; i < pairWordArray.length; i++) {
			          word.set(pairWordArray[i]);
			          occurrenceMap.clear();

			          int start = i;
			          int end = i+1;
			          if(start != pairWordArray.length -1){
			           for (int j = start; j <= end; j++) {
			                if (j == i) continue;
			                Text neighbor = new Text();
			                if(pairWordArray[i].compareTo(pairWordArray[j])>0){
								word.set(pairWordArray[j]);
								neighbor = new Text(pairWordArray[i]);
							}else{
								neighbor = new Text(pairWordArray[j]);
							}
			                if(occurrenceMap.containsKey(neighbor)){
			                   IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
			                   count.set(count.get()+1);
			                }else{
			                   occurrenceMap.put(neighbor,one);
			                }
			           }
			          context.write(word,occurrenceMap);
			     }
				}
	   }
	}
}