package io.prophecy.pipelines.candorandomthingsbuddy.graph

import io.prophecy.libs._
import io.prophecy.pipelines.candorandomthingsbuddy.udfs.PipelineInitCode._
import io.prophecy.pipelines.candorandomthingsbuddy.udfs.UDFs._
import io.prophecy.pipelines.candorandomthingsbuddy.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object CustomGemRepartitionJoinSplit_1 {
  def apply(context: Context, in: DataFrame): DataFrame = in.coalesce(10)
}
