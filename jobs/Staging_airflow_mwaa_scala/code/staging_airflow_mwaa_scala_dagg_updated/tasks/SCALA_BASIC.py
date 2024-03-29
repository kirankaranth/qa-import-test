def SCALA_BASIC():
    from datetime import timedelta
    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator # noqa

    return DatabricksSubmitRunOperator(  # noqa
        task_id = "SCALA_BASIC",
        json = {
          "task_key": "SCALA_BASIC", 
          "new_cluster": {
            "ssh_public_keys": [], 
            "node_type_id": "i3.xlarge", 
            "spark_version": "13.3.x-scala2.12", 
            "runtime_engine": "STANDARD", 
            "num_workers": 1.0, 
            "data_security_mode": "SINGLE_USER", 
            "custom_tags": {"billing" : "qa", "qa_usage_type" : "sanity"}, 
            "spark_conf": {
              "spark.prophecy.metadata.job.uri": "__PROJECT_ID_PLACEHOLDER__/jobs/Staging_airflow_mwaa_scala", 
              "spark.prophecy.metadata.is.interactive.run": "false", 
              "spark.prophecy.metadata.fabric.id": "25", 
              "spark.prophecy.tasks": "{\"SCALA_BASIC\":\"pipelines/SCALA_BASIC\"}", 
              "spark.prophecy.metadata.url": "__PROPHECY_URL_PLACEHOLDER__", 
              "spark.prophecy.project.id": "__PROJECT_ID_PLACEHOLDER__", 
              "spark.prophecy.execution.metrics.disabled": True, 
              "spark.prophecy.metadata.job.branch": "__PROJECT_RELEASE_VERSION_PLACEHOLDER__", 
              "spark.prophecy.execution.service.url": "wss://execution.staging.prophecy.io/eventws"
            }, 
            "init_scripts": [], 
            "aws_attributes": {
              "ebs_volume_count": 0.0, 
              "availability": "SPOT_WITH_FALLBACK", 
              "first_on_demand": 1.0, 
              "spot_bid_price_percent": 100.0, 
              "zone_id": "auto"
            }, 
            "spark_env_vars": {"PYSPARK_PYTHON" : "/databricks/python3/bin/python3"}
          }, 
          "spark_jar_task": {
            "main_class_name": "com.scala.main.job12.Main", 
            "parameters": ["-i", "default", "-O", "{}"]
          }, 
          "libraries": [{"maven" : {"coordinates" : "io.prophecy:prophecy-libs_2.12:3.4.0-7.1.46"}},                          {"pypi" : {"package" : "prophecy-libs==1.7.3"}},                          {
                           "jar": "dbfs:/FileStore/prophecy/artifacts/staging/cp/__PROJECT_ID_PLACEHOLDER__/__PROJECT_RELEASE_VERSION_PLACEHOLDER__/pipeline/SCALA_BASIC.jar"
                         }]
        },
        databricks_conn_id = "databricks_default",
    )
