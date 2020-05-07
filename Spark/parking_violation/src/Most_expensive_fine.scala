import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object Most_expensive_fine{

  def main(args : Array[String]) {
    val hadoopconf = new Configuration();
    hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);
    val fileSystem = FileSystem.get(hadoopconf);

    //spark configuration
    val conf = new SparkConf().setAppName("Most_expensive_fine");
    val sc = new SparkContext(conf);

    val reg="""^[-+]?(/d+(/./d*)?|/./d+)([eE]([-+]?([012]?/d{1,2}|30[0-7])|-3([01]?[4-9]|[012]?[0-3])))?[dD]?$""".r;

    val yearcount = sc.textFile(args(0)).map(_.split(",")).filter(x => (reg.findFirstMatchIn(x(13)) != None))
        .map(title=>(title(13).toDouble,title)).sortByKey().coalesce(1,true).saveAsTextFile(args(1));
    println("Word Count program running results are successfully saved.");

  }

}


