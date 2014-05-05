package part2;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairPartitioner extends Partitioner<Text, IntWritable> {
	 
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {

        String [] wordPair = key.toString().split(":");
        Character[] array1 = {'A','B','C','D','E','F','G','H','I','J','K','I','J'};
        Character[] array2 = {'L','M','N','O','P','Q','R','S','T','U','V','W','X'};
        ArrayList<Character> aList1 = new ArrayList<Character>(Arrays.asList(array1));
        ArrayList<Character> aList2 = new ArrayList<Character>(Arrays.asList(array2));
        if(wordPair.length==0){
        	wordPair = new String[1];
        	wordPair[0]="A";
        }
        if(aList1.contains(wordPair[0].charAt(0))){
            return 0;
        }else if(aList2.contains(wordPair[0].charAt(0))){
            return 1;
        }else{
        	return 2;
        }
    }
}