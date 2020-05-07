import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.util.matching.Regex

object Min_three{

  def main(args : Array[String]) {
    val hadoopconf = new Configuration();
    hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);
    val fileSystem = FileSystem.get(hadoopconf);

    //spark configuration
    val conf = new SparkConf().setAppName("Min_three");
    val sc = new SparkContext(conf);

    def parseRecord1(line:String): (Int,String)={
        var record = line;
        var res = record.split(",");

        res(0) = res(0).split("\\(")(1);
        res(1) = res(1).split("\\)")(0);
        var num = res(1).toInt*(-1);
        
        return (num,res(0));
    }

    var results = sc.textFile(args(0)).filter(x=>x.split(",")(0).contains("\\(") && x.split(",")(1).contains("\\)")).map(parseRecord1).coalesce(1,true).sortByKey().take(3);
    sc.parallelize(results).saveAsTextFile(args(1))

    

  }

}


