import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Most_common_cancellation_reason_mapper extends  Mapper <Object, Text, IntWritable, Text>{
	@Override        
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split("\t");
		context.write(new IntWritable(-1 * Integer.parseInt(items[1])), new Text(items[0]));
	}
}
