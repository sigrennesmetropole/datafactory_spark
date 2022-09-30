#!/bin/bash
usage() {
    cat << __EOF__
usage: `basename $0` options

Executing script for sheduling process

OPTIONS:
  -h Show this message
  -w window of time you want to analyse (set as 15 by default)
  -d date you want to execute the jobs (ex : 2021-01-01 | yyyy-mm-dd) (mandatory)
__EOF__
}

WINDOW=15

while getopts "hd:r:" OPTION ; do
        case $OPTION in
                h)
                        usage
                        exit 0
                        ;;
                d)
                        NOW=$OPTARG
                        ;;
                r)
                        REPRISE=$OPTARG
                        ;;
                *)
                        echo "Unknown option $OPTION"
                        ;;
        esac
done


############################
#     Main                 #
############################

red=$'\e[1;31m'
orange=$'\e[1;33m'
grn=$'\e[1;32m'
blu=$'\e[1;34m'
end=$'\e[0m'

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
#cd $SCRIPTPATH
LORAANALYSISPROPERTIES="$SCRIPTPATH/../conf/specific/rm-lora-analysis.properties"
source "${SCRIPTPATH}/../conf/specific/rm-lora-analysis.properties"
if [ $REPRISE == "reprise" ]
then
        printf "${gre}SPARK-LORA REPRISE via cronicle NOW:$NOW{end}\n"
        printf "docker exec ${CONTAINER_SPARK} spark-submit --num-executors=${TOTAL_EXEC_CORES} --executor-memory ${EXEC_MEMORY} --total-executor-cores ${TOTAL_EXEC_CORES} --driver-memory ${DRIVER_MEMORY} --class ${sparkClassReprise} --files ${sparkLoraConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file=${sparkLoraConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkLoraConfigurationPath} ${sparkJar} $NOW"
        docker exec ${CONTAINER_SPARK} spark-submit --num-executors=${TOTAL_EXEC_CORES} --executor-memory ${EXEC_MEMORY} --total-executor-cores ${TOTAL_EXEC_CORES} --driver-memory ${DRIVER_MEMORY} --class ${sparkClassReprise} --files ${sparkLoraConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file=${sparkLoraConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkLoraConfigurationPath} ${sparkJar} $NOW  
else
printf "${gre}SPARK-LORA via cronicle NOW:$NOW{end}\n"
printf "docker exec ${CONTAINER_SPARK} spark-submit --num-executors=${TOTAL_EXEC_CORES} --executor-memory ${EXEC_MEMORY} --total-executor-cores ${TOTAL_EXEC_CORES} --driver-memory ${DRIVER_MEMORY} --class ${sparkClass} --files ${sparkLoraConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file=${sparkLoraConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkLoraConfigurationPath} ${sparkJar} $NOW"
docker exec ${CONTAINER_SPARK} spark-submit --num-executors=${TOTAL_EXEC_CORES} --executor-memory ${EXEC_MEMORY} --total-executor-cores ${TOTAL_EXEC_CORES} --driver-memory ${DRIVER_MEMORY} --class ${sparkClass} --files ${sparkLoraConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file=${sparkLoraConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkLoraConfigurationPath} ${sparkJar} $NOW     
fi                                                                                                                                                                                          
result=$?

if [ $result -ne 0 ];
then
        printf "${red}Something went wrong, problem when running Lora job ! ${end}\n"
    exit 1
else
echo Lora job has been runned successfuly
fi
printf "${grn}End : Executing of Lora job DONE ${end}\n"