import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.util.matching.Regex

object Preprocess{

  def main(args : Array[String]) {
    val hadoopconf = new Configuration();
    hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);
    val fileSystem = FileSystem.get(hadoopconf);

    //spark configuration
    val conf = new SparkConf().setAppName("Preprocess");
    val sc = new SparkContext(conf);
    
    def parseRecord(line:String): String={
        val re = """\\"(.*?)\\"""".r;
        var record = line;

        for(matchString <- re.findAllIn(record)){
            val subject = matchString.replaceAll(",",";");
            record = re.replaceFirstIn(record,subject);   
            
        }
        record = record.replace(",,", ",NULL,");
        record = record.replace(",,", ",NULL,");
        record = record.replace(",,", ",NULL,");
        if(record.endsWith(",")) record = record + "NULL";
        
        return record
    }

    val results = sc.textFile(args(0)).map(parseRecord);

    results.coalesce(1,true).saveAsTextFile(args(1));

  }

}

