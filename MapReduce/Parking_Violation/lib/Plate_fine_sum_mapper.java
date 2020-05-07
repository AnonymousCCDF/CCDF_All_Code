import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Plate_fine_sum_mapper extends  Mapper <Object, Text, Text, DoubleWritable>{

    @Override
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] items = line.split(",");
                boolean flag = true;
                try{
                        Double.parseDouble(items[9]);
                } catch(NumberFormatException nfe){
                        flag = false;
                }
                if(flag){
                        DoubleWritable fine_amount = new DoubleWritable(Double.parseDouble(items[9]));
                        context.write(new Text(items[0]),fine_amount);
                }
        }
}
