package runner

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, Trigger}

object StructuredStream {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    if (args.length < 2) {
      System.err.println("Usage: SampleStream <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession
      .builder
      .master("local")
      .appName("SampleStream")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()
      .withColumn("timestamp",current_timestamp())
      .withWatermark("timestamp", "10 minutes")


    // Split the lines into words
    val words = lines.select(split(col("value")," ").as("words"), $"timestamp")
      .drop("value")
      .select($"timestamp",explode($"words").as("words"))


    case class Count(count: Long)

    import java.sql.Timestamp
    import org.apache.spark.sql.streaming.GroupState
    val keyCounts = (key: Long, values: Iterator[(Timestamp, Long)], state: GroupState[Count]) => {
      println(s""">>> keyCounts(key = $key, state = ${state.getOption.getOrElse("<empty>")})""")
      println(s">>> >>> currentProcessingTimeMs: ${state.getCurrentProcessingTimeMs}")
      println(s">>> >>> currentWatermarkMs: ${state.getCurrentWatermarkMs}")
      println(s">>> >>> hasTimedOut: ${state.hasTimedOut}")
      val count = Count(values.length)
      Iterator((key, count))
    }

    // Generate running word count
    val wordCounts =words.groupBy(
      window($"timestamp", "10 minutes", "30 seconds"),
      $"words"
    ).count()
    .withColumn("start_bin", (unix_timestamp(col("window.start")) / 300).cast("int"))
    .withColumn("end_bin", (unix_timestamp(col("window.end")) / 300).cast("int"))

    case class InputRow(word:String, start_bin:java.sql.Timestamp, end_bin:java.sql.Timestamp, count: Long)

    import java.sql.Timestamp
    import org.apache.spark.sql.streaming.GroupState
    import spark.implicits._
    /*val keyCounts = (key: (Long, Long), values: Iterator[(String, Long)], state: GroupState[Count]) => {
      println(s""">>> keyCounts(key = $key, state = ${state.getOption.getOrElse("<empty>")})""")
      println(s">>> >>> currentProcessingTimeMs: ${state.getCurrentProcessingTimeMs}")
      println(s">>> >>> currentWatermarkMs: ${state.getCurrentWatermarkMs}")
      println(s">>> >>> hasTimedOut: ${state.hasTimedOut}")
      val count = Count(values.length)
      Iterator((key, count))
    }

    wordCounts.as[InputRow].groupByKey(c => (c.start_bin, c.end_bin))
      .flatMapGroupsWithState(
        OutputMode.Update,
        timeoutConf = GroupStateTimeout.EventTimeTimeout)(func = keyCounts)
      .toDF("value", "count")
*/

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("update")
      .option("truncate", false)
      .format("console")
      .start()

    query.awaitTermination()
  }

}
