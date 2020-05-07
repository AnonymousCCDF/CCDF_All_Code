import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Average_taxi_time_mapper extends  Mapper <Object, Text, Text, IntWritable>{
    
    @Override
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split(",");
		Text flight_num = new Text(items[9]);
		boolean flag = true;
		try{
			Integer.parseInt(items[19]);
			Integer.parseInt(items[20]);
		} catch(NumberFormatException nfe){
			flag = false;
		}
		if(flag){
			IntWritable taxi_time = new IntWritable(Integer.parseInt(items[20])- Integer.parseInt(items[19])); 
			context.write(flight_num,taxi_time);
		}
	}
}