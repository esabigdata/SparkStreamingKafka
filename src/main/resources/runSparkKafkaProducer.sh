

jars=#required jar paths
files=#any_conf_file to pass
spark-submit --master yarn --deploy-mode cluster  --conf spark.yarn.maxAppAttempts=10 --name SparkKakfaProducer --num-executors 10 --executor-cores 3 --jars $jars --files $files --class com.sample.stream.SparkKafaProducer <path_to>jar>