import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Double_count_reducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
        double sum = 0;
        for(DoubleWritable val: values){
                sum += val.get();
        }
        context.write(key,new DoubleWritable(sum));
    }
  }

