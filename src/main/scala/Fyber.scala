import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType, TimestampType}

/**
  * Created by david on 17/07/2017.
  */
object Fyber {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("Fyber")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate


    val dfInput=spark
      .read
      .format("text")
      .option("header",false)
      .load("src\\main\\resources\\data_scala.txt")


    val cleanedDF=dfInput.withColumn("_tmp", split(col("value"),"\t"))
      .select(

        from_unixtime(col("_tmp").getItem(0)).cast(TimestampType).as("time"),
        col("_tmp").getItem(1).cast(DoubleType)as("price"),
        col("_tmp").getItem(0).as("originTime")
      )


    val timeColumn = window(col("time"), "59 seconds")
    val timePartition=Window.partitionBy(timeColumn).orderBy("originTime")

    cleanedDF.select(
      col("originTime").as("T"),
      col("price").as("V"),
      count("price").over(timePartition).as("N"),
      round(sum(col("price")).over(timePartition),5).as("RS"),
      min(col("price")).over(timePartition).as("MinV"),
      max(col("price")).over(timePartition).as("MaxV")
    ).orderBy("T")
     .show(false)

  }
 }
