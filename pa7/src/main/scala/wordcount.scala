import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object wordcount
{
  def main(args: Array[String])
  {
    val inputFilePath = "/home/gseu/pa7_assignment/input.txt" 
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFilePath).cache()
    val counter = textFile.flatMap(line => line.split(" "))
    val rdd = counter.map(word => (word, 1)).reduceByKey(case (x, y) => x + y)
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}