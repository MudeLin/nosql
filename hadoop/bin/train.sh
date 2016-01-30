SPARK_HOME=/home/hadoop/hadoop_nfs/spark-1.5.2-bin-hadoop2.6
CODE_HOME=/home/hadoop/nosql/hadoop/src
LOG_HOME=/home/hadoop/nosql/hadoop/log
$SPARK_HOME/bin/spark-submit  --files $CODE_HOME/util.py $CODE_HOME/train.py > $LOG_HOME/train.log

#$SPARK_HOME/bin/spark-submit --master local --num-executors 4 ../src/wet_LDA.py
