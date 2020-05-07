import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Most_expensive_fine_mapper     extends Mapper<Object, Text,  DoubleWritable,Text>{

     @Override
     public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {

          String line = value.toString();
          String[] items = line.split(",");
          boolean flag = true;
          try{
                  Double.parseDouble(items[13]);
          } catch(NumberFormatException nfe){
                  flag = false;
          }
          if(flag){
                  DoubleWritable payment_amount = new DoubleWritable(-1*Double.parseDouble(items[13]));
                  context.write(payment_amount,value);
          }



      }
  }


