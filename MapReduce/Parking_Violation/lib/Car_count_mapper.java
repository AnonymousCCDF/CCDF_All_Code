import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Car_count_mapper
       extends Mapper<Object, Text, Text, IntWritable>{

     @Override
     public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {

          String line = value.toString();
          String[] items = line.split(",");
		


          if(!items[0].equals("Plate") ){
                  context.write(new Text(items[0]),new IntWritable(1));
          }
      }
  }

