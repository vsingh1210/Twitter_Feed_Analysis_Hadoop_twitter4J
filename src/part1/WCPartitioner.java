package part1;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WCPartitioner extends Partitioner<Text, IntWritable>{
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {

        String [] wordPair = key.toString().split(":");
        wordPair[0]=wordPair[0].toUpperCase();
        //this is done to avoid performing mod with 0
        Character[] array1 = {'A','B','C','D','E','F','G','H','I','J','K','I','J','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};
        
        Character[] array2 = {'1','2','3','4','5','6','7','8','9','0'};
        ArrayList<Character> aList1 = new ArrayList<Character>(Arrays.asList(array1));
        ArrayList<Character> aList2 = new ArrayList<Character>(Arrays.asList(array2));
        if(aList1.contains(wordPair[0].charAt(0))){
            return 0;
        }else if(aList2.contains(wordPair[0].charAt(0))){
            return 1;
        }else if(wordPair[0].charAt(0)=='#'){
            return 2;
        }else{
        	return 3;
        }
    }
}
