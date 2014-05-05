package part2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeReducer extends Reducer<Text, MapWritable, Text, Text> {
    private MapWritable incrementingMap = new MapWritable();
    private Text word = new Text();
    private HashMap<String, Integer> hMap = new HashMap<>();
    private DoubleWritable tCount = new DoubleWritable();
    private DoubleWritable relCount = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        incrementingMap.clear();
        int count=0;
        for (MapWritable value : values) {
            int count2 = addAll(value);
            count+=count2;
        }
        tCount.set(count);
        @SuppressWarnings("rawtypes")
		Iterator it = hMap.entrySet().iterator();
        while (it.hasNext()) {
            @SuppressWarnings("rawtypes")
			Map.Entry pairs = (Map.Entry)it.next();
            relCount.set(Double.parseDouble(pairs.getValue().toString())/tCount.get());
            word.set(pairs.getKey() + "  " + relCount.get());
            context.write(key, word);
        }
        hMap.clear();
    }

    private int addAll(MapWritable mapWritable) {
        Set<Writable> keys = mapWritable.keySet();
        int count = 0;
        for (Writable key : keys) {
        	count++;
            IntWritable fromCount = (IntWritable) mapWritable.get(key);
            if(hMap.containsKey(key.toString())){
            	int cnt = hMap.get(key.toString());
            	hMap.put(key.toString(), cnt+fromCount.get());
            }else{
            	hMap.put(key.toString(), fromCount.get());
            }
        }
        return count;
    }
}