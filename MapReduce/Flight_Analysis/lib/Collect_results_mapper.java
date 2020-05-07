import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; //get the file name of input split
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Collect_results_mapper extends  Mapper <Object, Text, Text, Text>{
    @Override
    public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
        Path path = ((FileSplit) context.getInputSplit()).getPath(); 
        String fileName = path.toString();
        String[] tokens = fileName.split("/");
        Text word = new Text();
        Text out_key = new Text();
        for(String token : tokens){
            if(token.equals("max_three_aver_taxi_time")){
                String[] flight_info = value.toString().split("\\t");
                String output = "Flight Number: " + flight_info[0] + ", Average Saxi Time: " + flight_info[1];
                out_key.set("Max three average taxi time |");
                word.set(output);
                context.write(out_key, word);
            }else if(token.equals("min_three_aver_taxi_time")){
                String[] flight_info = value.toString().split("\\t");
                String output = "Flight Number: " + flight_info[0] + ", Average Saxi Time: " + flight_info[1];
                out_key.set("Min three average taxi time |");
                word.set(output);
                context.write(out_key, word);
            }else if(token.equals("max_three_on_schedule_rate")){
                String[] flight_info = value.toString().split("\\t");
                String output = "Airline Company" + flight_info[0] + ", On Schedule Rate: " + flight_info[1];
                out_key.set("Max three on schedule rate |");
                word.set(output);
                context.write(out_key, word);
            }else if(token.equals("min_three_on_schedule_rate")){
                String[] flight_info = value.toString().split("\\t");
                String output = "Airline Company: " + flight_info[0] + ", On Schedule Rate: " + flight_info[1];
                out_key.set("Min three on schedule rate |");
                word.set(output);
                context.write(out_key, word);
            }else if(token.equals("most_common_cancellation_reason")){
                out_key.set("Most common cancellation reason: ");
                context.write(out_key, value);
            }
        }
    }
}



