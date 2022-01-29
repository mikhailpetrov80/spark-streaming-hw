import com.sun.jmx.mbeanserver.Util.cast
import org.apache.kafka.common.serialization.{IntegerDeserializer, LongDeserializer}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.catalyst.StructFilters
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

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

  object KafkaLongDeserializer extends LongDeserializer
  object KafkaIntegerDeserializer extends IntegerDeserializer

  spark.udf.register("deserLong", (bytes: Array[Byte]) => KafkaLongDeserializer.deserialize(topic, bytes))
  spark.udf.register("deserInt", (bytes: Array[Byte]) => KafkaIntegerDeserializer.deserialize(topic, bytes))


  val records: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic)
    .load()
    .selectExpr( "deserLong(key) AS key_long", "deserInt(value) AS value_int")
    .agg(avg("value_int"))

  val query = records.writeStream
    .outputMode("update")
    .format("console")
    .trigger(trigger = ProcessingTime(30.seconds))
    .start()

  query.awaitTermination()

}
