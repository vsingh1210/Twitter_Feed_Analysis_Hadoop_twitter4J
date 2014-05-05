package part3;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeans{
	static enum centroid {
        centroidValue,
        centroidOneVal,
        centroidTwoVal,
        centroidThreeVal,
        itrCount,
        keyDisplay
    }
	private static final transient Logger LOG = LoggerFactory.getLogger(KMeans.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();		
		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		conf.set("c1", args[0]);
		conf.set("c2", args[1]);
		conf.set("c3", args[2]);
		String inputPath = "/input";
		String outputPath = "/outputKM";
		deleteFolder(conf,outputPath);
		boolean converged = false;
		boolean jobStatus = false;
		String infile = inputPath+"/"+args[3];
		String outputfile=outputPath;
		int count=0;
		while(!converged){
			Job job = Job.getInstance(conf);
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(infile));
			FileOutputFormat.setOutputPath(job, new Path(outputfile));
			jobStatus=job.waitForCompletion(true);
			converged = true;
			Counters jobCntrs = job.getCounters();
			if(jobCntrs.findCounter(centroid.centroidValue).getValue()>0 && count<25){
				converged = false;
				try {
					Path ofile = new Path(outputfile+"/part-r-00000");
					FileSystem fs = FileSystem.get(new Configuration());
		            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(ofile)));
					String cStr = br.readLine();
					String cValue[] = cStr.split("\\s+");
					conf.set("c1", cValue[0]);
					cStr = br.readLine();
					cValue = cStr.split("\\s+");
					conf.set("c2", cValue[0]);
					cStr = br.readLine();
					cValue = cStr.split("\\s+");
					conf.set("c3", cValue[0]);
					br.close();
					} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					}
				deleteFolder(conf,outputPath);
			}
			count++;
			jobCntrs.findCounter(centroid.centroidValue).setValue(0);
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