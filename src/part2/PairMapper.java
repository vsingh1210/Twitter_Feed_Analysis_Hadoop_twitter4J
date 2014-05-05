package part2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Text word2 = new Text();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		String pairWord="";
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken(" "));
			String tempstr = word.toString().toLowerCase();
			if(!tempstr.isEmpty() && tempstr.charAt(0)!='@' && !tempstr.contains("foll:") && !tempstr.contains("frnd:") && !tempstr.contains("rt@")){
				pairWord+=tempstr.toUpperCase()+":";
			}
		}
		if(pairWord.length()>0){
			pairWord = pairWord.substring(0,pairWord.length() - 1);
			String pairWordArray[]=pairWord.split(":");
			for(int i = 0; i< pairWordArray.length-1; i++){
				if(pairWordArray[i].compareTo(pairWordArray[i+1])<0){
					word.set(pairWordArray[i]+":"+pairWordArray[i+1]);
					word2.set(pairWordArray[i]+":*");
				}else if(pairWordArray[i].compareTo(pairWordArray[i+1])>0){
					if(pairWordArray[i].isEmpty()){
						word.set(pairWordArray[i+1]+":*");
					}else{
						word.set(pairWordArray[i+1]+":"+pairWordArray[i]);
						word2.set(pairWordArray[i+1]+":*");
					}
				}
				context.write(word, one);
				context.write(word2, one);
			}
			//word.set(pairWord);
		}
	}
}