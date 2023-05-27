import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object LogLevelGrouping extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "LogLevelGrouping")

  val input = sc.textFile("C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week10\\datasets/bigLog.txt")

  val mappedRDD = input.map(x => {
    val fields = x.split(":")
    (fields(0),fields(1))
  })

  mappedRDD.groupByKey.collect.foreach(x => println(x._1, x._2.size))

  //scala.io.StdIn.readLine()   to see DAG

}