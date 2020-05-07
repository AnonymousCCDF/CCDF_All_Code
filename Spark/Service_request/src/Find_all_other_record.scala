import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object Find_all_other_record{

  def main(args : Array[String]) {
    val hadoopconf = new Configuration();
    hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);
    val fileSystem = FileSystem.get(hadoopconf);

    //spark configuration
    val conf = new SparkConf().setAppName("Find_all_other_record");
    val sc = new SparkContext(conf);

    val yearcount = sc.textFile(args(0)).map(_.split(",")).filter(
        x=>x(16)!="QUEENS" && x(16)!="BROOKLYN" && x(16)!="BRONX" && x(16)!="MANHATTAN"
    ).coalesce(1,true).saveAsTextFile(args(1));
    println("Word Count program running results are successfully saved.");

  }

}


