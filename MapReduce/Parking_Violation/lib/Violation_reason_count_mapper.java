import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

  public  class Violation_reason_count_mapper
       extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String line = value.toString();
        String[] items = line.split(",");

        if(!items[6].equals("Violation")){
                Text violation_reason = new Text(items[6]);
                context.write(violation_reason,one);

        }
    }
  }
