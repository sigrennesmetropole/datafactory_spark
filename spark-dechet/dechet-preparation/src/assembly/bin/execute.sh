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
printf "${gre}docker exec ${CONTAINER_SPARK} spark-submit --master spark://spark:7077  --executor-cores 4 --executor-memory 1G --driver-memory 1G --class $SPARKCLASS --files ${sparkDechetConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} ${sparkJar} $NOW  ${end}\n"
logs=$(docker exec ${CONTAINER_SPARK} spark-submit --class $SPARKCLASS --files ${sparkDechetConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} ${sparkJar} $NOW) 
#docker exec ${CONTAINER_SPARK} spark-submit --class $SPARKCLASS --files ${sparkDechetConfigurationPath} --conf spark.driver.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} --conf spark.executor.extraJavaOptions=-Dconfig.file=${sparkDechetConfigurationPath} ${sparkJar} $NOW

result=$?
	printf "result execute : $result \n"
	printf "logs : $logs \n"
if [ $result -ne 0 ];
then

	printf "${red}Something went wrong, problem when running dechet analysis job ! ${end}\n"
	if [[ ( $logs == *"Erreur de chargement des fichiers depuis MinIO"* && $logs == *"No such file or directory:"* ) || ( $logs == *"Pas de donne a reprendre"* ) ]]
	then
		printf "Probablement pas de données dans minio..."
		exit 2 #code d'erreur pour signifier que c'est un manque de fichier en entrée
	fi
    exit $result
else
echo Dechet job has been runned successfuly
fi
printf "${grn}End : Executing of dechet preparation job DONE ${end}\n"
