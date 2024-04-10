def build_spark_submit():
    """ """
    # render spark-submit command from library
    # Ou até mesmo pode ser um comando hard-coded contanto que o nome do job, do modulo e odate sejam dinamicos

    return "spark-submit --class org.apache.spark.examples.SparkPi --deploy-mode client path/to/exemples/spark-examples*.jar 10"
