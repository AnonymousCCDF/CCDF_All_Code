import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Cancellation_reason_count_reducer extends  Reducer <Text, IntWritable, Text, IntWritable> {  
    @Override
    public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int count = 0;
		for(IntWritable val:values){
			count++;
		}
		context.write(key, new IntWritable(count) );
    }
}
