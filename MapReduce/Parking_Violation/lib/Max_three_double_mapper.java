import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Max_three_double_mapper extends  Mapper <Object, Text, DoubleWritable, Text>{
    @Override
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] items = line.split("\t");

		String result = "";
		for(int i = 0; i < items.length -1; i++){
			   result = result + items[i];
		}

                context.write(new DoubleWritable(-1 * Double.parseDouble(items[items.length-1])), new Text(result));
        }
}
