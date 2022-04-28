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

while getopts "he:a:w:d:" OPTION ; do
	case $OPTION in
		h)
			usage
			exit 1
			;;
		w)
			WINDOW=$OPTARG
			;;
		d) 
			NOW=$OPTARG
			;;
		*)
			echo "Unknown option $OPTION"
			;;
	esac
done
if (($WINDOW <= 0 ))		# check if the parameters window is coherent
then
	printf "Please specify a correct window, $WINDOW is not a valid window\n"
	exit 1
fi


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
TRAFFICANALYSISPROPERTIES="$SCRIPTPATH/../conf/specific/rm-traffic-analysis.properties"
source "${SCRIPTPATH}/../conf/specific/rm-traffic-analysis.properties"

printf "${gre}docker exec ${CONTAINER_SPARK} spark-submit --master ${MASTER_SPARK} --executor-memory ${EXEC_MEMORY} --total-executor-cores ${TOTAL_EXEC_CORES} --driver-memory ${DRIVER_MEMORY} --class ${sparkClass} ${sparkJar} $NOW $WINDOW$ {end}\n"
docker exec ${CONTAINER_SPARK} spark-submit --master ${MASTER_SPARK} --num-executors=20 --executor-memory ${EXEC_MEMORY} --total-executor-cores ${TOTAL_EXEC_CORES} --driver-memory ${DRIVER_MEMORY} --class ${sparkClass} ${sparkJar} $NOW $WINDOW 

result=$?

if [ $result -ne 0 ];
then
	printf "${red}Something went wrong, problem when running spark job ! ${end}\n"
    exit 1
else
echo spark job has been runned successfuly
fi
printf "${grn}End : Executing of Spark job DONE ${end}\n"
