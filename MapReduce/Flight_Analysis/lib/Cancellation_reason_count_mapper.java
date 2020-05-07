import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Cancellation_reason_count_mapper extends  Mapper <Object, Text, Text, IntWritable>{
    
    @Override
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split(",");
		if(items[22].equals("A") || items[22].equals("B") || 
			items[22].equals("C") || items[22].equals("D") ){
			context.write(new Text(items[22]), new IntWritable(1));
		}
	}
}
