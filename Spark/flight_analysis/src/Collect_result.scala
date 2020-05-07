import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object  Collect_results{

  def main(args : Array[String]) {
    val hadoopconf = new Configuration();
    hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);
    val fileSystem = FileSystem.get(hadoopconf);

    //spark configuration
    val conf = new SparkConf().setAppName("Collect_results");
    val sc = new SparkContext(conf);

    val yearcount = sc.textFile(args(0)).coalesce(1,true).saveAsTextFile(args(1));
    println("Word Count program running results are successfully saved.");
  }

}

