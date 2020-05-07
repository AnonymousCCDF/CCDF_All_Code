import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Max_three_average_taxi_time_reducer extends  Reducer <DoubleWritable, Text, Text, DoubleWritable> { 
	private static int linesum = 1; 
	@Override
    public void reduce (DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for(Text val: values){
			if(linesum <= 3){
				linesum++;
				context.write(val,new DoubleWritable(-1 * key.get()));
			}
		}
    }
}
