#! /bin/bash

function join { local IFS="$1"; shift; echo "$*"; }

appName=$(join - $*)

echo submit $appName

root=$PWD

# jars=$(join , `ls $PWD/targets/*jar`)
algojar=$root/target/rsp-algos-1.0-SNAPSHOT-jar-with-dependencies.jar

submitcmd="$SPARK_HOME/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--class com.szubd.rspalgos.App \
--jars $root/dependencies/spark-rsp_2.11-2.4.0.jar \
--conf spark.yarn.maxAppAttempts=0 \
--name $appName \
$algojar $*"

if [ ! -d "logs" ]
then
    mkdir logs
fi

echo `date "+%Y-%m-%dT%T"` $submitcmd >> logs/submit-commands-history.log

$SPARK_HOME/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--class com.szubd.rspalgos.App \
--jars $root/dependencies/spark-rsp_2.11-2.4.0.jar \
--conf spark.yarn.maxAppAttempts=0 \
--conf spark.driver.memory=16g \
--conf spark.executor.memory=16g \
--conf spark.executor.instances=10 \
--conf spark.executor.cores=1 \
--conf spark.driver.cores=1 \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=hdfs://nameservice1/user/spark/applicationHistory \
--name $appName \
$algojar $*
