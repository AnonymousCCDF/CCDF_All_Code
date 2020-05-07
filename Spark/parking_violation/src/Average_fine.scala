import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.util.matching.Regex

object Average_fine{

  def main(args : Array[String]) {
    val hadoopconf = new Configuration();
    hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);
    val fileSystem = FileSystem.get(hadoopconf);

    //spark configuration
    val conf = new SparkConf().setAppName("Average_fine");
    val sc = new SparkContext(conf);


    val reg="""^[-+]?(/d+(/./d*)?|/./d+)([eE]([-+]?([012]?/d{1,2}|30[0-7])|-3([01]?[4-9]|[012]?[0-3])))?[dD]?$""".r;
    
    var tax_time = sc.textFile(args(0)).map(_.split(",")).filter(x => (reg.findFirstMatchIn(x(9)) != None )
    ).map(x=>("Fine",(x(9).toDouble,1))).reduceByKey((a,b) => (a._1+b._1,a._2+b._2)
    ).map(t => (t._1,t._2._1/t._2._2)).saveAsTextFile(args(1));
    println("Word Count program running results are successfully saved.");

  }

}



