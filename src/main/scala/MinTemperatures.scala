import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.math.min

object MinTemperatures extends App {

  def parseLine(line:String) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3)
    (stationID, entryType, temperature)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "mintemp")

  val input = sc.textFile("C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week9/tempdata.csv")

  val parsedLines = input.map(parseLine)

  val minTemps = parsedLines.filter(x => x._2 == "TMIN")

  val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))

  val minTempByStation = stationTemps.reduceByKey((x,y) => min(x,y))

  val results = minTempByStation.collect()

  for (result <- results.sorted) {
     val station = result._1
     val temp = result._2
     val formattedTemp = f"$temp%.2f F"
     println(s"$station minimum temperature: $formattedTemp")
  }

}