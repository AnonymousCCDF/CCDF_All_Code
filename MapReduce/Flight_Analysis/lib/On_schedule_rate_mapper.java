import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class On_schedule_rate_mapper extends  Mapper <Object, Text, Text, IntWritable>{
      
    @Override  
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split(",");
		int x;
		boolean flag = true;
		try{
			Integer.parseInt(items[15]);
		} catch(NumberFormatException nfe){
			flag = false;
		}
		if(flag){
			if(Integer.parseInt(items[15]) == 0){
				x = 1;
			}else{
				x = 0;
			}
			context.write(new Text(items[8]), new IntWritable(x));
		}
	}
}