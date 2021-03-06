// this program has the code for (driver, mapper and reducer)

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Collect_results_reducer extends  Reducer <Text, Text, Text, Text> {
    @Override        
    public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{           
        for (Text val : values){
            context.write(key,val);
        }
    }
}
