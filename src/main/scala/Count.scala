/* 

$SPARK_HOME/bin/spark-submit\
  --packages datastax:spark-cassandra-connector:1.4.4-s_2.11\
  --driver-memory 4g
  keywordCount.jar
 
 
 */



import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

import java.time.LocalDateTime
import scala.util.matching.Regex
import scala.collection.JavaConverters._
import scala.collection.mutable._


object Count {

  def main(args: Array[String]){
    val states = Set("AL", "AK", "AZ", "AR", "CA", "CO", 
        "CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", 
        "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", 
        "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", 
        "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", 
        "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", 
        "VA", "WA", "WV", "WI", "WY", "USA")
    


    //val states = Set("CA")

    //Regex for parsing get_day.findFirstIn(str)
    val get_before_t = "^([^T]+)".r       
    val get_before_space = "^([^\\s]+)".r
    val get_before_delim = "^[^|]*".r
    val get_after_delim = "\\|(.*)".r

    lazy val conf = new SparkConf(true)
         .set("spark.cassandra.connection.host", "127.0.0.1")
         .set("spark.executor.memory", "3g") 
         .set("spark.executor.instances", "2")
         

    val sc = new SparkContext("spark://127.0.0.1:7077", "Count Job", conf)

    val today = LocalDateTime.now()
    val start_date = get_before_t.findFirstIn(today.minusDays(6).toString())


    //each row will look like (timestamp, (keywords, from, that, day))
    val twitter_keywords = sc.cassandraTable("main", "keywords")
      .select("timestamp", "keyword")
      .where("origin = ? and timestamp > ?", "Twitter Trends", start_date)
      .map(r => 
          (
            get_before_space.findFirstIn(r.get[String]("timestamp")).toString.dropRight(1).drop(5),
            r.get[String]("keyword").toLowerCase
          ))
      .groupByKey()
      .persist()
    
    val google_search_keywords = sc.cassandraTable("main", "keywords")
      .select("timestamp", "keyword")
      .where("origin = ? and timestamp > ?", "Google Search Trends", start_date)
      .map(r => 
          (
            get_before_space.findFirstIn(r.get[String]("timestamp")).toString.dropRight(1).drop(5),
            r.get[String]("keyword").toLowerCase
          ))
      .groupByKey()
      .persist()

    val google_hot_keywords = sc.cassandraTable("main", "keywords")
      .select("timestamp", "keyword")
      .where("origin = ? and timestamp > ?", "Google Hot Trends", start_date)
      .map(r => 
          (
            get_before_space.findFirstIn(r.get[String]("timestamp")).toString.dropRight(1).drop(5),
            r.get[String]("keyword").toLowerCase
          ))
      .groupByKey()
      .persist()
  
    for(state <- states){
      println("========================== " + state + " ===========================")
      //Get tweets
      val tweets = sc.cassandraTable[(String, String)]("main", "tweets")
        .select("timestamp", "message")
        .where("state = ? and timestamp > ?", " " + state, start_date)
        .map(r => 
            (
              get_before_space.findFirstIn(r._1).toString.dropRight(1).drop(5),
              (r._2).toLowerCase
            ))
        .persist()
        

      //Time to get matching keywords
      //After join (timestamp (message,(keyword,1,2,3)))
      // r._1    -> timestamp 
      // r._2._1 -> mese
      // r._2._2 -> list of keywords
      // Maybe a flatMap where this list is of the matching
      tweets
        .join(twitter_keywords)
        .flatMap(r =>  (r._2._2).filter(r._2._1.contains).map {case x => (r._1,x)})
        .map(r => (r,1))
        .reduceByKey(_+_)
        .map(r => ("Twitter Trends", r._1._1, state, r._1._2, r._2))
        .saveToCassandra("main", "data_twitter", 
            SomeColumns("origin", "timestamp", "state", "keyword", "tweets_num"))

      tweets
        .join(google_search_keywords)
        .flatMap(r =>  (r._2._2).filter(r._2._1.contains).map {case x => (r._1,x)})
        .map(r => (r,1))
        .reduceByKey(_+_)
        .map(r => ("Google Search Trends", r._1._1, state, r._1._2, r._2))
        .saveToCassandra("main", "data_twitter", 
            SomeColumns("origin", "timestamp", "state", "keyword", "tweets_num"))

      tweets
        .join(google_hot_keywords)
        .flatMap(r =>  (r._2._2).filter(r._2._1.contains).map {case x => (r._1,x)})
        .map(r => (r,1))
        .reduceByKey(_+_)
        .map(r => ("Google Hot Trends", r._1._1, state, r._1._2, r._2))
        .saveToCassandra("main", "data_twitter", 
            SomeColumns("origin", "timestamp", "state", "keyword", "tweets_num"))


      println("========================== " + state + " ===========================")

    }
    
    

    twitter_keywords.unpersist()
    
  }
}















