import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.util.matching.Regex

object On_schedule_rate{

  def main(args : Array[String]) {
    val hadoopconf = new Configuration();
    hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);
    val fileSystem = FileSystem.get(hadoopconf);

    //spark configuration
    val conf = new SparkConf().setAppName("On_schedule_rate");
    val sc = new SparkContext(conf);


    val reg="""^[1-9]\\d*$""".r;

    def parseRecord(line:String): String={
        val pattern = "\"(.*?)\"";
        val re = pattern.r;
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
    
    var tax_time = sc.textFile(args(0)).map(_.split(",")).filter(
        x => (reg.findFirstMatchIn(x(15)) != None)
    ).map(
        x => (x(8),((if(x(15).toInt == 0) 1 else 0),1))
    ).reduceByKey(
        (a,b) => (a._1+b._1,a._2+b._2)
    ).map(
        t => (t._1,t._2._1/t._2._2)
    ).saveAsTextFile(args(1));
    println("Word Count program running results are successfully saved.");

  }

}


