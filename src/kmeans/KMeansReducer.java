package kmeans;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, Text> {
	
	public void reducer(DoubleWritable key, Iterator<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		double newCentroid;
		double sum = 0;
		int counter = 0;
		while(values.hasNext()){
			double point = values.next().get();
			sum += point;
			counter++;
		}
		newCentroid = sum/counter;
		
		context.write(new DoubleWritable(newCentroid), new Text(""));
	}
}
