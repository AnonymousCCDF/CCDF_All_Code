import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Find_all_other_record_mapper
       extends Mapper<Object, Text, Text, Text>{

     @Override   
     public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {

          String line = value.toString();
          String[] items = line.split(",");
          if(!items[16].equals("BROOKLYN") && !items[16].equals("BRONX") && !items[16].equals("QUEENS") && 
		!items[16].equals("MANHATTAN") ){
                  context.write(value,new Text(""));
          }
      }
  }


