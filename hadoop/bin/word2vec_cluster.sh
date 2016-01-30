SPARK_HOME=/home/hadoop/hadoop_nfs/spark-1.5.2-bin-hadoop2.6
CODE_HOME=/home/hadoop/nosql/hadoop/src
LOG_HOME=/home/hadoop/nosql/hadoop/log
#$SPARK_HOME/bin/spark-submit  $CODE_HOME/preprocess.py > $LOG_HOME/log.log

$SPARK_HOME/bin/spark-submit --master yarn-cluster --num-executors 8 $CODE_HOME/preprocess.py > $LOG_HOME/log.log

