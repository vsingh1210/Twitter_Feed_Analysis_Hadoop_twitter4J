package part2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PairReducer 
extends Reducer<Text,IntWritable,Text,DoubleWritable> {
	private DoubleWritable tCount = new DoubleWritable();
	private DoubleWritable relCount = new DoubleWritable();
	private Text tempWord = new Text("");
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		String str = key.toString();
		String[] pair = str.split(":");
		if(pair.length<2){
			String temp = pair[0];
			pair = new String[2];
			pair[0]=temp;
			pair[1]="*";
		}
		String neighbor = pair[1];
		if(neighbor.compareTo("*")==0){
			if(neighbor.equals(tempWord)){
				tCount.set(tCount.get()+getTCount(values));
			}else{
				tempWord.set(pair[0]);
				tCount.set(0);
				tCount.set(getTCount(values));
			}
		}else{
			int count = getTCount(values);
			relCount.set((double) count / tCount.get());
			context.write(key, relCount);
		}
	}
	private int getTCount(Iterable<IntWritable> values) {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        return count;
    }
}
