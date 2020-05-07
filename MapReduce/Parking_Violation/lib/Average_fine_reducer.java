import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Average_fine_reducer extends  Reducer <Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce (Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
                int count = 0;
                double sum = 0;
                for(DoubleWritable val:values){
                        count++;
                        sum = sum + val.get();
                }
                context.write(key, new DoubleWritable((double)sum/count) );
    }
}
