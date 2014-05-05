package part4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShortestPath{
	static enum MoreIterations {
        convergenceCount
	}
	private static final transient Logger LOG = LoggerFactory.getLogger(ShortestPath.class);

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();		
		conf.set("mapred.textoutputformat.separator", " ");
		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		/* Set the Input/Output Paths on HDFS */
		String inputPath = "/inputGraph";
		String outputPath = "/outputGraph";
		boolean converged = false;
		boolean jobStatus = false;
		String infile = inputPath;
		String outputfile=outputPath+System.nanoTime();
		HashMap <Integer, Integer> hmap = new HashMap<>();
		deleteFolder(conf,outputPath);
		while(!converged){
			Job job = Job.getInstance(conf);
			job.setJarByClass(ShortestPath.class);
			job.setMapperClass(ShortestPathMapper.class);
			job.setReducerClass(ShortestPathReducer.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(infile));
			FileOutputFormat.setOutputPath(job, new Path(outputfile));
			jobStatus=job.waitForCompletion(true);
			if(infile != inputPath){
				String indir = infile.replace("part-r-00000", "");
				Path ddir = new Path(indir);
				FileSystem dfs = FileSystem.get(conf);
				dfs.delete(ddir,true);
			}
			infile = outputfile +"/part-r-00000";
			outputfile = outputPath + System.nanoTime();
			converged = true;
			Path ofile = new Path(infile);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));
			HashMap<Integer, Integer> imap = new HashMap<Integer, Integer>();
            String line=br.readLine();
            while (line != null){
                String[] sp = line.split("\\s+");
                int node = Integer.parseInt(sp[0]);
                int distance = Integer.parseInt(sp[1]);
                imap.put(node, distance);
                line=br.readLine();
            }
            if(hmap.isEmpty()){
                converged = false;
            }else{
                Iterator<Integer> itr = imap.keySet().iterator();
                while(itr.hasNext()){
                    int key = itr.next();
                    int val = imap.get(key);
                    if(hmap.get(key) != val){
                        converged = false;
                    }
                }
            }
            if(converged == false){
                hmap.putAll(imap);
            }
		}
			System.exit(jobStatus ? 0 : 1);
	}
	
	private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
}