import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Complain_count_mapper
       extends Mapper<Object, Text, Text, IntWritable>{

     @Override
     public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {

          String line = value.toString();
          String[] items = line.split(",");
          if(!items[5].equals("Complaint Type")){
                  context.write(new Text(items[5]),new IntWritable(1));
          }
      }
  }

