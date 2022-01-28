import org.apache.spark.sql
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession, types}

import scala.concurrent.duration.DurationInt

// https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
object SparkApp extends App {

  val spark = SparkSession.builder
    .appName("spark-streaming-hw")
    .master(sys.env.getOrElse("spark.master", "local[*]"))
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val brokers = "localhost:9092"
  val topic = "records"

  val records: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic)
    .load()
    .withColumn("key", records("key").cast(IntegerType))
    .withColumn("value", records("value").cast(IntegerType))
    //.select(records("key").cast(IntegerType).as("key"), records("value").cast(IntegerType).as("value"))
    .select("key", "value")

 /* val records1 = records
    .withColumn("Key", toInt(records("key")))
    .withColumn("Value", toInt(records("value")))
    .select("Key", "Value")
*/
  records.printSchema()

  val query = records.writeStream
    .outputMode("update")
    .format("console")
    .trigger(trigger = ProcessingTime(30.seconds))
    .start()

  query.awaitTermination()

}
