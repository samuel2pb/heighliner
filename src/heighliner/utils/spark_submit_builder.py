def build_spark_submit():
    """
    """
    odate = "{{ ds }}"
    # render spark-submit command from b3df library
    # Ou até mesmo pode ser um comando hard-coded contanto que o nome do job, do modulo e odate sejam dinamicos
    spark_submit = f"{odate}"

    return spark_submit
