import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Subject_count_mapper extends Mapper<Object, Text, Text, IntWritable>{
    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String line = value.toString();
        String[] items = line.split(",");

	if(items.length > 8){
	        if(!items[8].equals("Subjects")){
        	        String[] subjects = items[8].split(";");
	                for(int i = 0; i < subjects.length; i++){
                	    context.write(new Text(subjects[i]),new IntWritable(1));
        	        }
	        }
	}
    }
}

