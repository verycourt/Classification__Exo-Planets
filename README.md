# Spark-project
Shell command used to compile, at the build.sbt level:

sbt assembly

Shell command used to execute the code, at the spark-2.0.0-bin-hadoop2.6/bin/ level:

./spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp" --driver-memory 3G --executor-memory 4G --class com.sparkProject.JobML /Users/bertrrandBertrand/Desktop/MS_BIG_DATA/Spark/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar

-> this shows that the job used is JobML, and the execution is local.

We would have used 
./spark-submit --driver-memory 3G --executor-memory 4G --class com.sparkProject.Job --master spark:"path of the master node"  /Users/bertrrandBertrand/Desktop/MS_BIG_DATA/Spark/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar

where the path of the master is //eduroam-0-60.enst.fr:7077 for example.
