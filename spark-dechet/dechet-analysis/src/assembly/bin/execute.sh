#!/bin/bash
usage() {
    cat << __EOF__
usage: `basename $0` options

Executing script for sheduling process

OPTIONS:
  -h Show this message
  -d date you want to execute the jobs (ex : 2021-01-01 | yyyy-mm-dd) (mandatory)
  -t type of analysis (collecte or referential)(mandatory)
__EOF__
}

while getopts "hd:t:a" OPTION ; do
API=true
	case $OPTION in
		h)
			usage
			exit 0
			;;
		d)
			NOW=$OPTARG
			;;
		t) 
			TYPE=$OPTARG
			;;
		a) 
			API=$OPTARG
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
api=""

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
#cd $SCRIPTPATH
DECHETTRAFFICANALYSISPROPERTIES="$SCRIPTPATH/../conf/specific/rm-dechet-analysis.properties"
source "${SCRIPTPATH}/../conf/specific/rm-dechet-analysis.properties"

if [ $TYPE == "collecte" ];
then
	SPARKCLASS=${sparkClassCollecte}
elif [ $TYPE == "referentiel_prod" ];
then
	SPARKCLASS=${sparkClassRefProd}
elif [ $TYPE == "referentiel_recip" ];
then
	SPARKCLASS=${sparkClassRefRecip}
elif [ $TYPE == "exutoire" ];
then
	SPARKCLASS=${sparkClassExutoire}
elif [ $TYPE == "collecteExposure" ];
then
	SPARKCLASS=${sparkClassCollecteExposure}
elif [ $TYPE == "referentielExposure" ];
then
	SPARKCLASS=${sparkClassRefExposure}
	if ! $API 
	then
		api="false"
	fi
elif [ $TYPE == "collecteRedressement" ];
then
	SPARKCLASS=${sparkClassCollecteRedressement}
else
printf "${red}Something went wrong, we don't recognize the type of execution you want, you give $TYPE -> we want either collecte or ref ${end}\n"
exit 1
fi
printf "${gre}docker exec ${CONTAINER_SPARK} --num-executors=${TOTAL_EXEC_CORES} --executor-memory ${EXEC_MEMORY} --total-executor-cores ${TOTAL_EXEC_CORES} --driver-memory ${DRIVER_MEMORY} --class $SPARKCLASS --files ${sparkDechetConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file= ${sparkDechetConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} ${sparkJar} $NOW $api ${end}\n"
docker exec ${CONTAINER_SPARK} spark-submit --num-executors=${TOTAL_EXEC_CORES} --executor-memory ${EXEC_MEMORY} --total-executor-cores ${TOTAL_EXEC_CORES} --driver-memory ${DRIVER_MEMORY} --class $SPARKCLASS --files ${sparkDechetConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} ${sparkJar} $NOW $api



result=$?

if [ $result -ne 0 ];
then
	printf "${red}Something went wrong, problem when running dechet analysis job ! ${end}\n"
    exit 1
else
echo Dechet job has been runned successfuly
fi
printf "${grn}End : Executing of dechet analysis job DONE ${end}\n"
