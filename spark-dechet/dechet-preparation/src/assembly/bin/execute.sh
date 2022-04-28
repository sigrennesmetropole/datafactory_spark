#!/bin/bash
usage() {
    cat << __EOF__
usage: `basename $0` options

Executing script for sheduling process

OPTIONS:
  -h Show this message
  -d date you want to execute the jobs (ex : 2021-01-01 | yyyy-mm-dd) (mandatory)
  -t type of preparation (collecte or referential) (mandatory)
__EOF__
}

while getopts "hd:t:" OPTION ; do
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

DECHETTRAFFICANALYSISPROPERTIES="$SCRIPTPATH/../conf/specific/rm-dechet-preparation.properties"
source "${SCRIPTPATH}/../conf/specific/rm-dechet-preparation.properties"

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
else
printf "${red}Something went wrong, we don't recognize the type of execution you want, you give $TYPE -> we want either collecte or ref ${end}\n"
exit 0
fi
printf "${gre}docker exec ${CONTAINER_SPARK} spark-submit --class $SPARKCLASS --files ${sparkDechetConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file= ${sparkDechetConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} ${sparkJar} $NOW  ${end}\n"
docker exec ${CONTAINER_SPARK} spark-submit --class $SPARKCLASS --files ${sparkDechetConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} ${sparkJar} $NOW 


result=$?

if [ $result -ne 0 ];
then
	printf "${red}Something went wrong, problem when running dechet preparation job ! ${end}\n"
    exit 1
else
echo Dechet job has been runned successfuly
fi
printf "${grn}End : Executing of dechet preparation job DONE ${end}\n"
