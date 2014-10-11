package kmeans;

import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {
	
	private static final int MAX_ITERATION = 3;
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		int iteration = 0;
		
		String inputPath = "/input";
		String outputPath = "/output";
		
		String centroidDir = "/centroids";
		
		while(iteration < MAX_ITERATION){
			
			Configuration conf = new Configuration();
			
			FileSystem hdfs = FileSystem.get(conf);
			Path path = new Path(centroidDir);
			FileStatus[] fstatuses = hdfs.listStatus(path);
			
			for(FileStatus fstatus : fstatuses){
				if(fstatus.getPath().toUri().getPath().contains("/_")){
					continue;
				}
				conf.set("centroids", fstatus.getPath().toUri().getPath());
			}
			
			
			Job job = Job.getInstance(conf);
			job.setJobName("KMeans Iteration " + (iteration+1));
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath + iteration));
			
			centroidDir = outputPath + iteration;
			
			int exitStatus = job.waitForCompletion(true)?0:1;
			
			if(exitStatus != 0){
				System.exit(exitStatus);
			}
			
			iteration++;
		}
	}

}
