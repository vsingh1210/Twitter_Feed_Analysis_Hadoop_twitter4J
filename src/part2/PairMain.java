package part2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PairMain {
	private static final transient Logger LOG = LoggerFactory.getLogger(PairMain.class);

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();		

		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		/* Set the Input/Output Paths on HDFS */
		String inputPath = "/input";
		String outputPath = "/outputPair";
		boolean jobStatus = false;
		/* FileOutputFormat wants to create the output directory itself.
		 * If it exists, delete it:
		 */
		deleteFolder(conf,outputPath);
		
		Job job = Job.getInstance(conf);

		job.setJarByClass(PairMain.class);
		job.setMapperClass(PairMapper.class);
		//job.setCombinerClass(PairReducer.class);
		job.setPartitionerClass(PairPartitioner.class);
		job.setNumReduceTasks(3);
		job.setReducerClass(PairReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		jobStatus=job.waitForCompletion(true);
		//Sort reducer output 0
		Path ifile = new Path(outputPath+"/part-r-00000");
		FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(ifile)));
        Map<Double, ArrayList<String>> map=new TreeMap<>();
        String line="";
        
        while((line=br.readLine())!=null){
        	ArrayList<String> aList = new ArrayList<>();
        	String[] op = line.split("\\s+");
        	if(map.containsKey(Double.parseDouble(op[1]))){
        		aList=map.get(Double.parseDouble(op[1]));
        		aList.add(op[0]);
        		map.put(Double.parseDouble(op[1]),aList);
        	}else{
        		aList.add(op[0]);
        		map.put(Double.parseDouble(op[1]),aList);
        	}
        	
        }
        br.close();
        Path ofile = new Path(outputPath+"/part-r-00000-sorted");
		fs = FileSystem.get(new Configuration());
        FSDataOutputStream out = fs.create(ofile);
        for(Entry<Double, ArrayList<String>> entry : map.entrySet()) {
        	String key = entry.getKey().toString();
        	ArrayList<String> al = entry.getValue();
        	for(int i = 0; i<al.size(); i++){
        		String temp = al.get(i) + "  " + key;
        		out.writeBytes(temp);
            	out.write('\n');
        	}
        }
        out.close();
		//Sort reducer output 1
  		ifile = new Path(outputPath+"/part-r-00001");
  		fs = FileSystem.get(new Configuration());
        br=new BufferedReader(new InputStreamReader(fs.open(ifile)));
        map=new TreeMap<>();
        line="";
	    while((line=br.readLine())!=null){
	     	ArrayList<String> aList = new ArrayList<>();
	      	String[] op = line.split("\\s+");
	      	if(map.containsKey(Double.parseDouble(op[1]))){
	      		aList=map.get(Double.parseDouble(op[1]));
	      		aList.add(op[0]);
	      		map.put(Double.parseDouble(op[1]),aList);
	      	}else{
	      		aList.add(op[0]);
	      		map.put(Double.parseDouble(op[1]),aList);
	     	}
	    }
	    br.close();
        ofile = new Path(outputPath+"/part-r-00001-sorted");
		fs = FileSystem.get(new Configuration());
	    out = fs.create(ofile);
	    for(Entry<Double, ArrayList<String>> entry : map.entrySet()) {
	     	String key = entry.getKey().toString();
	      	ArrayList<String> al = entry.getValue();
	      	for(int i = 0; i<al.size(); i++){
	      		String temp = al.get(i) + "  " + key;
	      		out.writeBytes(temp);
	          	out.write('\n');
	      	}
	      }
	      out.close();
        //
	    //Sort reducer output 2
	  		ifile = new Path(outputPath+"/part-r-00002");
	  		fs = FileSystem.get(new Configuration());
	        br=new BufferedReader(new InputStreamReader(fs.open(ifile)));
	        map=new TreeMap<>();
	        line="";
		    while((line=br.readLine())!=null){
		     	ArrayList<String> aList = new ArrayList<>();
		      	String[] op = line.split("\\s+");
		      	if(map.containsKey(Double.parseDouble(op[1]))){
		      		aList=map.get(Double.parseDouble(op[1]));
		      		aList.add(op[0]);
		      		map.put(Double.parseDouble(op[1]),aList);
		      	}else{
		      		aList.add(op[0]);
		      		map.put(Double.parseDouble(op[1]),aList);
		     	}
		    }
		    br.close();
	        ofile = new Path(outputPath+"/part-r-00002-sorted");
			fs = FileSystem.get(new Configuration());
		    out = fs.create(ofile);
		    for(Entry<Double, ArrayList<String>> entry : map.entrySet()) {
		     	String key = entry.getKey().toString();
		      	ArrayList<String> al = entry.getValue();
		      	for(int i = 0; i<al.size(); i++){
		      		String temp = al.get(i) + "  " + key;
		      		out.writeBytes(temp);
		          	out.write('\n');
		      	}
		      }
		      out.close();
	        //
		System.exit(jobStatus ? 0 : 1);
	}
	
	/**
	 * Delete a folder on the HDFS. This is an example of how to interact
	 * with the HDFS using the Java API. You can also interact with it
	 * on the command line, using: hdfs dfs -rm -r /path/to/delete
	 * 
	 * @param conf a Hadoop Configuration object
	 * @param folderPath folder to delete
	 * @throws IOException
	 */
	private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
}