package SparkApps

/**
  * Created by root on 6/6/17.
  */
//import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.metrics.source

import scala.collection.mutable.SynchronizedQueue
import scala.io.Source
import scala.util.parsing.json.JSON
//import scala.

/*object StreamingJson {
  def main(args: Array[String]): Unit = {
    //val sparkSession = SparkSession.builder().appName("stream json").getOrCreate()
    
    /*val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    // Here we read data line by line from a given file and then put it into a queue DStream.
    // You can replace any kind of String type DStream here including kafka DStream.
    val queue = new SynchronizedQueue[RDD[String]]()
    Source.fromFile("/root/IdeaProjects/spark-lambda/payload_examples.json").getLines().foreach(msg =>
      queue.enqueue(sc.parallelize(List(msg))))
    val queueDStream = ssc.queueStream[String](queue)
    // We can infer the schema of json automatically by using inferJsonSchema
    val schema = streamSqlContext.inferJsonSchema("/root/IdeaProjects/spark-lambda/payload_examples.json")
    streamSqlContext.registerDStreamAsTable(
      streamSqlContext.jsonDStream(queueDStream, schema), "jsonTable")
    sql("SELECT * FROM jsonTable").print()*/
    //ssc.start()
    //ssc.awaitTerminationOrTimeout(30 * 1000)
   //ssc.stop()


  }
}
*/