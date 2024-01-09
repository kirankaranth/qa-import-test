package io.prophecy.pipelines.candorandomthingsbuddy

import io.prophecy.libs._
import io.prophecy.pipelines.candorandomthingsbuddy.config.Context
import io.prophecy.pipelines.candorandomthingsbuddy.config._
import io.prophecy.pipelines.candorandomthingsbuddy.udfs.UDFs._
import io.prophecy.pipelines.candorandomthingsbuddy.udfs._
import io.prophecy.pipelines.candorandomthingsbuddy.udfs.PipelineInitCode._
import io.prophecy.pipelines.candorandomthingsbuddy.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_dataset_cust_in = dataset_cust_in(context)
    val df_CustomGemRepartitionJoinSplit_1 =
      CustomGemRepartitionJoinSplit_1(context, df_dataset_cust_in)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/CanDoRandomThingsBuddy")
    registerUDFs(spark)
    try MetricsCollector.start(spark,
                               "pipelines/CanDoRandomThingsBuddy",
                               context.config
    )
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/CanDoRandomThingsBuddy")
    }
    apply(context)
    MetricsCollector.end(spark)
  }

}
