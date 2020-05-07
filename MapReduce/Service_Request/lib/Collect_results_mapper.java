import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Collect_results_mapper extends  Mapper <Object, Text, Text, Text>{
    @Override
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
	context.write(value,new Text(""));
    }
}

