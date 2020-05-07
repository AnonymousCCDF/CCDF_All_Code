import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Max_three_reducer extends  Reducer <IntWritable, Text, Text, IntWritable> {
        private static int linesum = 1;
        @Override
    public void reduce (IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
                for(Text val: values){
                        if(linesum <= 3){
                                linesum++;
                                context.write(val,new IntWritable(-1 * key.get()));
                        }
                }
    }
}

