jars=#required jar paths
files=#any_conf_file to pass
spark-submit --master yarn --deploy-mode cluster  --conf spark.yarn.maxAppAttempts=1 --name SparkKakfaConsumer --num-executors 15 --executor-cores 4 --executor-memory 20G --driver-memory 30G --conf spark.yarn.executor.memoryOverhead=16G --driver-java-options -XX:MaxPermSize=15G --jars $jars --files $files --class com.sample.batch.BatchProcessing <path_to>jar>
