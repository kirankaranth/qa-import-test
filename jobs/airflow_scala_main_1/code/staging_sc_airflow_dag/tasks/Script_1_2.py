def Script_1_2():
    from datetime import timedelta
    from airflow.operators.python import PythonOperator

    return PythonOperator(
        task_id = "Script_1_2",
        python_callable = lambda *args, **kwargs: exec("print(\"hello !\")"),
    )
