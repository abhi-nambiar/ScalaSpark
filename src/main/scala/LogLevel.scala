import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object LogLevel extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "LogLevel")

  //local list residing on the machine, not cluster
  val myList = List("WARN: Tuesday 4 September 0405",
                   "ERROR: Tuesday 4 September 0408",
                   "ERROR: Tuesday 4 September 0408",
                   "ERROR: Tuesday 4 September 0408",
                   "ERROR: Tuesday 4 September 0408",
                   "ERROR: Tuesday 4 September 0408")

  //to create an rdd from a list, we cannot use sc.textFile
  //parallelize will create rdd from local col
  val originalLogsRDD = sc.parallelize(myList)

  //we want to have output like (WARN,1), (ERROR,1), (ERROR,1), (ERROR,1), (ERROR,1), (ERROR,1)
  //using map to do this
  val newPaidRDD = originalLogsRDD.map(x => {
    val columns = x.split(":")
    val logLevel = columns(0)
    (logLevel,1)
  })

  //to find total count of warn and error
  val resultantRDD = newPaidRDD.reduceByKey((x,y) => x + y)

  resultantRDD.collect.foreach(println)

}