import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object TopMovies extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "topmovies")

  val ratingsRDD = sc.textFile("C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week11\\datasets/ratings.dat")

  //get movie id and rating from file
  val mappedRDD = ratingsRDD.map(x => {
                    val fields = x.split("::")
                    (fields(1),fields(2))
                  })
  //eg: (1193,4), (1193,5)

  val newMapped = mappedRDD.mapValues(x => (x.toFloat,1.0))
  //(1193,(4.0,1.0)), (1193,(5.0,1.0))

  val reducedRDD = newMapped.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
  //(1193,(9.0,2.0))  -> total rating and no of people rated
  //we need movies rated by more than 1000 people

  val filteredRDD = reducedRDD.filter(x => x._2._2 > 100)
  //all values with more than 100 people rated will be selected

  val ratingsProcessed = filteredRDD.mapValues(x => x._1/x._2).filter(x => x._2 > 4.5)
  //all movies with rating more than 4.5 are selected
  //eg: (1193,4.6)

  val moviesRDD = sc.textFile("C:\\Users\\Lenovo\\Desktop\\Documents\\abhi\\big data\\week11\\datasets/movies.dat")
  //other file with movie names

  val moviesMapped = moviesRDD.map(x => {
    val fields = x.split("::")
    (fields(0),fields(1))
  })
  //movie id and name are selected from file
  //(1193,Toy story)

  val joinedRDD = moviesMapped.join(ratingsProcessed)
  //(1193,(Toy story,4.6))

  val topMovies = joinedRDD.map(x => x._2._1)
  //Toy story

  topMovies.collect.foreach(println)
  //we get the final list of movies rated more than 100 times with a rating of over 4.5

  //scala.io.StdIn.readLine()

}