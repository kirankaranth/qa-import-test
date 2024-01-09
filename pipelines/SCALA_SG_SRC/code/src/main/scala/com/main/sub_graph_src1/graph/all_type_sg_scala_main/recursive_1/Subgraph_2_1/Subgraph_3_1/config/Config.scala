package com.main.sub_graph_src1.graph.all_type_sg_scala_main.recursive_1.Subgraph_2_1.Subgraph_3_1.config

import org.apache.spark.sql._
import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import com.main.sub_graph_src1.graph.all_type_sg_scala_main.recursive_1.Subgraph_2_1.Subgraph_3_1.Subgraph_4_1.config.{
  Config => Subgraph_4_1_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

  implicit val interimOutput: InterimOutput = InterimOutputHive2("")
}

case class Config(Subgraph_4_1: Subgraph_4_1_Config = Subgraph_4_1_Config())
    extends ConfigBase

object DatabricksSecret {

  implicit val myIntReader: ConfigReader[DatabricksSecret] =
    ConfigReader[String].map { s =>
      val Array(scope, key) = s.split(":")
      DatabricksSecret(scope, key)
    }

}

case class DatabricksSecret(scope: String, key: String) {

  override def toString: String = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    dbutils.secrets.get(scope = scope, key = key)
  }

}

case class Context(spark: SparkSession, config: Config)
