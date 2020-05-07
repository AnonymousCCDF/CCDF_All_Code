import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Most_common_cancellation_reason_reducer extends  Reducer <IntWritable, Text, Text, NullWritable> { 
	private static int linesum = 1; 
	@Override
    public void reduce (IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for(Text val: values){
			if(linesum <= 1){
				linesum++;
				String reason = "No Cancellation";
				if(val.toString().equals("A")){
					reason = "Carrier";
				}
				if(val.toString().equals("B")){
                                        reason = "Weather";
                                }
				if(val.toString().equals("C")){
                                        reason = "NAS";
                                }
				if(val.toString().equals("D")){
                                        reason = "Security";
                                }
				context.write(new Text(reason),NullWritable.get());
			}
		}
    }
}
    
    