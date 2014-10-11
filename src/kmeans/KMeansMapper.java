package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, DoubleWritable, DoubleWritable>{
	
	public static List<Double> centroids = new ArrayList<Double>();
	
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		FileSystem hdfs = FileSystem.get(conf);
		
		if(conf.get("centroids")==null){
			throw new RuntimeException("centroids file not present");
		}
		
		
		Path centPath = new Path(conf.get("centroids"));
		FSDataInputStream fs = hdfs.open(centPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs));
		
		String line = null;
		while((line = br.readLine())!=null){
			centroids.add(Double.parseDouble(line));
		}
		
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		double point = Double.parseDouble(value.toString());
		
		double diff = Double.MAX_VALUE, nearestCentroid = centroids.get(0);
		
		for(double centroid : centroids){
			if(Math.abs(point - centroid)<diff){
				diff = Math.abs(point - centroid);
				nearestCentroid = centroid;
			}
		}
		context.write(new DoubleWritable(nearestCentroid), new DoubleWritable(point));
	}
}