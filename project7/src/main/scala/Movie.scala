import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Movie 
{
  

  def main(args: Array[ String ]) 
  {
    val conf = new SparkConf().setAppName("Movie")
    val sc = new SparkContext(conf)

    var movies = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                (a(0),a(1).toDouble,a(2)) } ).cache

    var genre = movies.map{t => (t._1,1)}.groupByKey
    var result1 = genre.map(t => { var m = 0
                                      for(v <- t._2)
                                        {
                                          m = m + v
                                        }
                                        (t._1,m)
                                        }).collect().sorted

    println("Genre, Number_of_movies")                                  
    result1.foreach(t => println(t))
    println()
    
    var year = movies.map{t => (t._3,1)}.groupByKey
    var result2 = year.map(t => { var m = 0
                                      for(v <- t._2)
                                        {
                                          m = m + v
                                        }
                                        (t._1,m)
                                        }).collect().sorted
    println("Year, Number_of_movies")                                    
    result2.foreach(t => println(t))
    println()
    
    var year2 = movies.map{t => ((t._3,t._1),1)}.groupByKey
 
    var result3 = year2.map(t => { var m = 0
                                      for(v <- t._2)
                                        {
                                          m = m + v
                                        }
                                        (t._1,m)
                                        }).collect().sorted
    println("Year, Genre, Number_of_movies")                                    
    result3.foreach(t => println(t))
    println()
    
    var year3 = movies.map{t => ((t._3,t._1),t._2)}.groupByKey
 
    var result4 = year3.map(t => { var m = 0.0
                                   var count = 0
                                   var mean = 0.0
                                   for(v <- t._2)
                                   {
                                          m = m + v
                                          count = count + 1
                                   }
                                        
                                   mean = m/count
                                   (t._1,m,mean)
                                   }).collect().sorted
    
    println("Year, Genre, Cummulative_Rating_Score, Mean_Rating_ScoreS")                                    
    result4.foreach(t => println(t))
    
    sc.stop()
  }
}
