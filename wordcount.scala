package SparkApps

/**
  * Created by root on 3/6/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTimeZone}
import org.joda.time.format.DateTimeFormat

object wordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("json").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val payload = sqlContext.read.json("/root/IdeaProjects/spark-lambda/payload_examples.json")
    payload.printSchema()
    payload.registerTempTable("payload")
    //val payload_sql = sqlContext.sql("select count(event.id) from payload HAVING hour(CAST(meta.ts_received AS TIMESTAMP))=3,minute(CAST(meta.ts_received AS TIMESTAMP))=22")
    val payload_sql = sqlContext.sql("select count(event.id),day(CAST(meta.ts_received AS TIMESTAMP)),hour(CAST(meta.ts_received AS TIMESTAMP)),minute(CAST(meta.ts_received AS TIMESTAMP)) from payload GROUP BY day(CAST(meta.ts_received AS TIMESTAMP)),hour(CAST(meta.ts_received AS TIMESTAMP)),minute(CAST(meta.ts_received AS TIMESTAMP))")
    payload_sql.collect().foreach(println)
  }
}