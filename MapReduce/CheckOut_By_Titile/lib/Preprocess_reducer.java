import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Preprocess_reducer
     extends Reducer<Text,Text,Text,Text> {
  @Override
  public void reduce(Text key, Iterable<Text> values,
                     Context context
                     ) throws IOException, InterruptedException {
    for (Text val : values) {
      context.write(key,new Text(val));
    }
  }
}
