package io.prophecy.pipelines.scinterimsdatabrickspipeline.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scinterimsdatabrickspipeline.udfs.PipelineInitCode._
import io.prophecy.pipelines.scinterimsdatabrickspipeline.udfs.UDFs._
import io.prophecy.pipelines.scinterimsdatabrickspipeline.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object SQLStatement_2_1 {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    in0.createOrReplaceTempView("in0")
    context.spark.sql("select * from in0")
  }

}
