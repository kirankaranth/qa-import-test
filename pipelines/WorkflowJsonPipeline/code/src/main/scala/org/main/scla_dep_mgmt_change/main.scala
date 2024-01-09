package org.main.scla_dep_mgmt_change

import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.Context
import org.main.scla_dep_mgmt_change.config._
import org.main.scla_dep_mgmt_change.config.ConfigStore.interimOutput
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.udfs._
import org.main.scla_dep_mgmt_change.udfs.PipelineInitCode._
import org.main.scla_dep_mgmt_change.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def graph(context: Context): Unit = {
    val df_src_avro_CustsDatasetInput_1 = src_avro_CustsDatasetInput_1(context)
      .interim("graph", "A7YBjCffwys4LPAleAKpC", "bsEyEzvvLYNF6CFSyeX9o")
    if (context.config.c_array_complex(0).car_record.carr_short > -10)
      Lookup_1(context, df_src_avro_CustsDatasetInput_1)
    val df_Script_1 =
      Script_1(context, df_src_avro_CustsDatasetInput_1).interim(
        "graph",
        "zvL4eQufcf7JXDWE5naBz$$FaOjEEhgZ-ohY-GFzOtkz",
        "zvrtFJPhpuoVXq8RQ6UgB$$oj1NDu1sKgKz7Ldy31y9n"
      )
    val df_call_func = call_func(context, df_Script_1).interim(
      "graph",
      "jDGCmYurLPi5p2PI0NGES$$ppPGE4WaX6-Zibw1VKZeH",
      "kcQDaPwg6yVi98DcYU8IQ$$IoQqIwKAkSnY95_gSy2rx"
    )
    df_call_func.count()
    val df_src_json_input_custs_1 = src_json_input_custs_1(context)
      .interim("graph", "XM4cdlXB7oVFseHwX2LRg", "gB7zngP2OXebTsbxfm4vF")
      .cache()
    df_src_json_input_custs_1.count()
    val df_src_snowflake = src_snowflake(context).interim(
      "graph",
      "52UHk_L27-2XPH7dmbKDG$$5rCEbTpHn-snIHnc3jslU",
      "hP_QZgulZQ0dARPbkRuoI$$EAkuZYLgyNPMQJCq7cpSY"
    )
    val df_Reformat_7 = Reformat_7(context, df_src_snowflake).interim(
      "graph",
      "9aa6ZtBggCK2Cjy_kaR4q$$zeHZEG4Txup06A6pPklUA",
      "DNBPYkVnbHRSEFFPI0eC7$$Da86mF3SrqXuO_XJ8f3M9"
    )
    val df_CustomGemTransformFilterCategory_1 =
      CustomGemTransformFilterCategory_1(context, df_Reformat_7).interim(
        "graph",
        "BDEJ_KZiFDF2KTtPm4ozu$$0dYY3s8yDj-Lgblg40QAp",
        "-IdkZMCELIPhu2-tslk7-$$jmsHIh9_c3WddsUpRX_ul"
      )
    val df_CustomGemRepartitionJoinSplit_1 =
      CustomGemRepartitionJoinSplit_1(context,
                                      df_CustomGemTransformFilterCategory_1
      ).interim("graph",
                "kI2b46wNPw58JlDRl-6lV$$LFKMvYQfVC57p7t3Rnxif",
                "ZzPJoSYbPzTpMCuXbQaiz$$L5J7qaG56RxjlDDKi7WL4"
      )
    val df_CustomReformat_1 =
      CustomReformat_1(context, df_CustomGemRepartitionJoinSplit_1).interim(
        "graph",
        "CcvDLcAiyY1YYZ9HC7vWk$$8JojvXzw8dUfGbrQUT6Or",
        "Bnx5K3TKp_J_kVMfj2dCl$$crOvi-SaSAby1C1BYGTgD"
      )
    val df_CustomJoin_1 =
      CustomJoin_1(context, df_CustomReformat_1, df_CustomReformat_1).interim(
        "graph",
        "bE1cvt4h8v8GjZgiMPfK1$$OYmqbLm9XLVp3akyb3LCu",
        "vakqUWqmtmItcFPtLn9BO$$sI7nlm-gXI_0SpY_fpiUh"
      )
    df_CustomJoin_1.count()
    val df_src_parquet_all_type_no_partition =
      src_parquet_all_type_no_partition(context)
        .interim("graph", "PM7sxRmKo0cGk1IYdBNtT", "zMejsXna2UN-uClx0tfKO")
        .cache()
    val df_ComplexExpression = ComplexExpression(
      context,
      df_src_parquet_all_type_no_partition
    ).interim("graph", "2tRCXGkA-6TfjEnFofIJq", "MwSfNu4URv3g0PC7kc9CR")
    val df_OrderBy_3 = OrderBy_3(context, df_ComplexExpression).interim(
      "graph",
      "hx5wO_87IAH8xNU8kd6u0$$NZNcKwMNB77oH_rUMhHw2",
      "3QwZLdav6axGl_0dxx9N_$$CvmPmUKZnuBwQ1mj-VrK0"
    )
    val df_src_parquet_all_type_and_partition_withspacehyphens =
      src_parquet_all_type_and_partition_withspacehyphens(context).interim(
        "graph",
        "4c8lUyCOBMQXJt_5f7dbP$$hbMC_bA0Uq2x2q8FW4tAX",
        "pIPX9BUJ7XgmTvGgmV8Hu$$y6Sz1JeyfULGcGgi8slvs"
      )
    val df_Filter_10 = Filter_10(
      context,
      df_src_parquet_all_type_and_partition_withspacehyphens
    ).interim("graph", "V7Jv1tslSD7wNcPsY3dLt", "QLJWIrn_EA9HKXGU2Hg7d")
    val df_WindowFunction_1 = WindowFunction_1(context, df_Filter_10).interim(
      "graph",
      "kHjXzB0HTJD1XTuwrj5kw$$ufL5LdEj0VdfG7-f1lHjQ",
      "Xh1IgHG5x-W2wGzFMUf9N$$f3xyINARrBYxOtt8dLXCx"
    )
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) = {
      val (df_RowDistributor_1_out0_temp, df_RowDistributor_1_out1_temp) =
        RowDistributor_1(context, df_WindowFunction_1)
      (df_RowDistributor_1_out0_temp
         .interim("graph",
                  "mxj3GIDr8L_wSjsWZxCzw$$ly00a3LzUT_45X1UWdYHu",
                  "out0"
         )
         .cache(),
       df_RowDistributor_1_out1_temp
         .interim("graph",
                  "mxj3GIDr8L_wSjsWZxCzw$$ly00a3LzUT_45X1UWdYHu",
                  "out1"
         )
         .cache()
      )
    }
    val df_OrderBy_4 = OrderBy_4(context, df_RowDistributor_1_out0).interim(
      "graph",
      "hWAQiNOptWzlVEkn-huuJ$$q-JVLZQnREXaW_I25BSj5",
      "Z4XurMdUJO-ETinTrSyh6$$CxMPDJXpvul_WH8wvOY5y"
    )
    df_OrderBy_4.count()
    val df_Limit_3 = Limit_3(context, df_RowDistributor_1_out1).interim(
      "graph",
      "Fveo5Vzi24BOiUGy55PZU$$9-p9OnU3R-r06567Tejez",
      "Ft6cGNpKOE2Nirw_XS9_R$$Cd22cRsabCIorU61swO2N"
    )
    df_Limit_3.count()
    val df_ConfigAndUDF =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        ConfigAndUDF(context, df_Script_1).interim("graph",
                                                   "ryf6nWZatrJJgaQGDWPjC",
                                                   "bY6dBUB7OHy6i8vc1uwbD"
        )
      else df_Script_1
    if (context.config.c_array_complex(0).car_record.carr_short > -10) {
      val df_Filter_2 = Filter_2(context, df_ConfigAndUDF).interim(
        "graph",
        "F_cxTDso7G28ruB0xni7N",
        "V_c7nCHezVT96rpb-8TzQ"
      )
      val df_OrderBy_2 = OrderBy_2(context, df_Filter_2).interim(
        "graph",
        "0BEbuoCU7vasdz7Wr3Ft1",
        "1DyiAt45y3SZCoDDCNVmw"
      )
      withSubgraphName("graph", context.spark) {
        withTargetId("dest_test", context.spark) {
          dest_test(context, df_OrderBy_2)
        }
      }
    }
    val df_Aggregate_1 = Aggregate_1(context, df_OrderBy_3)
      .interim("graph",
               "n0VmJXrJcJhCDBbma0KdJ$$k94j1JSMRlVwaZ6r7RhGb",
               "Q5r1wB6YPEzdTGEVZN4dU$$Z-m7r-v8lekG4XOCQmIO2"
      )
      .cache()
    df_Aggregate_1.count()
    val df_src_csv_special_char_column_name =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        src_csv_special_char_column_name(context)
          .interim("graph", "nEj64p7qzVS7z0LXXTFkx", "2G70-QEVG04zcV_iAsqv1")
          .cache()
      else null
    val df_Limit_1 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        Limit_1(context, df_src_csv_special_char_column_name).interim(
          "graph",
          "BtjgWEFk-IrCWsqN3RqDF",
          "vz8yOBdktTG02eFYUsCr_"
        )
      else null
    if (df_Limit_1 != null)
      df_Limit_1.count()
    val df_UTGenRepartition_1 =
      if (
        context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10 && context.config
          .c_array_complex(0)
          .car_record
          .carr_short > -10
      )
        if (context.config.c_array_complex(0).car_record.carr_short > -10)
          UTGenRepartition_1(context, df_src_csv_special_char_column_name)
            .interim("graph",         "JDdGnXnYzeiw8aXJ5gB5q", "sNPPOS1ix-ZWOvC-Q0cPD")
        else df_src_csv_special_char_column_name
      else null
    if (df_UTGenRepartition_1 != null)
      df_UTGenRepartition_1.count()
    val df_Repartition_2 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        Repartition_2(context, df_WindowFunction_1).interim(
          "graph",
          "yfeifaX7xpRlj28Ls-8Vf$$29PiihbU95u1gXWccO3GA",
          "xIA_sDLdYIaSR9jB4VXlg$$Rv5ZSnqWe9vse2SDVvmu_"
        )
      else df_WindowFunction_1
    df_Repartition_2.count()
    val df_UTGenOrderBy_1 =
      if (context.config.c_array_complex(0).car_record.carr_short > -10)
        UTGenOrderBy_1(context, df_src_csv_special_char_column_name).interim(
          "graph",
          "hxZRArGTe6IeA715uZ9hX",
          "NGuAUZuZAsYsYcFmYLp2B"
        )
      else null
    if (df_UTGenOrderBy_1 != null)
      df_UTGenOrderBy_1.count()
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
    MetricsCollector.initializeMetrics(spark)
    implicit val interimOutputConsole: InterimOutput = InterimOutputHive2("")
    spark.conf.set("prophecy.collect.basic.stats",          "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules",
                   "org.apache.spark.sql.catalyst.optimizer.ColumnPruning"
    )
    spark.conf.set("spark_config1",
                   "spark./<>;'\"[]{}\\|~*/-+p- config1 value !~_#@%^&*()-="
    )
    spark.conf.set("spark_config2",     "spark_config2_value")
    spark.conf.set("fs.s3a.access.key", "AKIAR6ESAR2JAQNZNVMH")
    spark.conf
      .set("fs.s3a.secret.key", "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")
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/WorkflowJsonPipeline")
    spark.sparkContext.hadoopConfiguration.set(
      "hadoop_config1",
      "hadoop./<>;'\"[]{}\\|~*/-+p- config1 value !~_#@%^&*()-="
    )
    spark.sparkContext.hadoopConfiguration
      .set("hadoop_config2", "hadoop_config2_value")
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.access.key", "AKIAR6ESAR2JAQNZNVMH")
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.secret.key", "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")
    try MetricsCollector.start(spark,
                               "pipelines/WorkflowJsonPipeline",
                               context.config
    )
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/WorkflowJsonPipeline")
    }
    graph(context)
    MetricsCollector.end(spark)
  }

}
