
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public  class Preprocess_mapper
       extends Mapper<Object, Text, Text, Text>{

     @Override
     public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {
          String line = value.toString();

          Pattern p=Pattern.compile("\"(.*?)\"");
          Matcher m=p.matcher(line);
          while(m.find())
          {
                  String str = m.group();
                  line = line.replace(str,str.replace(",",";"));
          }

          line = line.replace(",,", ",NULL,");
          line = line.replace(",,", ",NULL,");
          line = line.replace(",,", ",NULL,");
          if(line.endsWith(",")){
                  line = line + "NULL";
          }

          context.write(new Text(line),new Text(""));
        }
}


