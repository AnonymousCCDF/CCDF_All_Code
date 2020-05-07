import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object Book_title_count{

  def main(args : Array[String]) {
    val hadoopconf = new Configuration();
    hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);
    val fileSystem = FileSystem.get(hadoopconf);

    //spark configuration
    val conf = new SparkConf().setAppName("Book_title_count");
    val sc = new SparkContext(conf);

    println("QIANWEN: book Count start.");
    val bookcount = sc.textFile(args(0)).map(_.split(",")).filter(x=>x(6)!="Title").map(title=>(title(6),1)).reduceByKey(_+_).coalesce(1,true).saveAsTextFile(args(1));
    println("QIANWEN: book Count program running results are successfully saved.");

  }

}




