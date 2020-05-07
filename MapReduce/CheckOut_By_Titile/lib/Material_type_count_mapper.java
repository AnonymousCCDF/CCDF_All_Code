import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


  public class Material_type_count_mapper extends Mapper<Object, Text, Text, IntWritable>{

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String line = value.toString();
        String[] items = line.split(",");
        if(!items[2].equals("MaterialType")){
                Text material_type_num = new Text(items[2]);
                context.write(material_type_num,new IntWritable(1));

        }
    }
  }
