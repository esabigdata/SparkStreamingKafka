

jars=#required jar paths
files=#any_conf_file to pass
spark-submit --master yarn --deploy-mode cluster  --conf spark.yarn.maxAppAttempts=10 --name SparkKakfaConsumer --num-executors 3 --executor-cores 3 --jars $jars --files $files --class com.sample.stream.SparkKafaConsumer <path_to>jar>