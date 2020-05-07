import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Max_three_mapper extends  Mapper <Object, Text, IntWritable, Text>{
    @Override
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] items = line.split("\t");

String result = "";
  for(int i = 0; i < items.length -1; i++){
   result = result + items[i];
  }

                context.write(new IntWritable(-1 * Integer.parseInt(items[items.length-1])), new Text(result));
        }
}
