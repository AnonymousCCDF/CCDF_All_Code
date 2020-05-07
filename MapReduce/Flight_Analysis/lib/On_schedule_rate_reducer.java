import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public  class On_schedule_rate_reducer extends  Reducer <Text, IntWritable, Text, DoubleWritable> {
	@Override  
    public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int count = 0;
		int sum = 0;
		for(IntWritable val:values){
			count++;
			sum = sum + val.get();
		}
		context.write(key, new DoubleWritable((double)sum/count) );
    }
}