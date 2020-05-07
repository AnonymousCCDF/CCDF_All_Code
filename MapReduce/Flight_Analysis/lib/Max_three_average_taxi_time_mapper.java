import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Max_three_average_taxi_time_mapper extends  Mapper <Object, Text, DoubleWritable, Text>{
    @Override
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split("\t");
		context.write(new DoubleWritable(-1 * Double.parseDouble(items[1])), new Text(items[0]));
	}
}
