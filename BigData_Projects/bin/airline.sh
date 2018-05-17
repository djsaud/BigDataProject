#!/usr/bin/env bash
####################################################################
# AUTHOR : SAMPAT BUDANKAYALA                                      #
# DESC : THIS SCRIPT POPULATES THE AIRLINE DATA AND STORE IN AVRO  #
# CREATE DATE :                                                    #
# MODIFIED DATE :                                                  #
# MODIFIED DESC :                                                  #
####################################################################


# Extract SCRIPT_HOME
SCRIPT_PATH="${BASH_SOURCE[0]}";
SCRIPT_HOME=`dirname $SCRIPT_PATH`

PROJECT_HOME=`dirname ${SCRIPT_HOME}`
COMMON_HOME="${PROJECT_HOME}/common"
ENV_HOME="${PROJECT_HOME}/etc"
PIG_HOME="${PROJECT_HOME}/pig"

export ENV_HOME;

# Load namespace property file

. ${COMMON_HOME}/shell_functions/namespace_functions.sh
. ${COMMON_HOME}/shell_functions/common_functions.sh

info "[COMMON_HOME_PATH] $COMMON_HOME"
SCRIPT_START_TIME=$(getFormattedCurrentDate)
info "[SCRIPT_START_TIME] $SCRIPT_START_TIME"
user_namespace_param_file=$(getNamespaceFileForCurrentUser)
info "[NAMESPACE_PROP_FILE_NAME] $user_namespace_param_file"

# Load default environmental property files
info "-------------SOURCE NAMESPACE PROPERTIES-----------------"
. ${ENV_HOME}/namespace/${user_namespace_param_file}
. ${ENV_HOME}/project/project.env.properties
. ${ENV_HOME}/project/project.pig.properties


#############READING PARAMETERS##################
info "--------------STARTED PARSING ARGUMENTS-------------------"
while [[ $# > 1 ]]
do
PARAM="$1"

case ${PARAM} in
    -q|--mrq)
    MAPREDUCE_QUEUE_NAME="$2"
    shift # past argument
    ;;
    -t|--table)
    TABLE_NAME="$2"
    shift # past argument
    ;;
    -i|--inputpath)
    INPUT_PATH="$2"
    shift # past argument
    ;;
    -o|--outputpath)
    OUTPUT_PATH="$2"
    shift # past argument
    ;;
    -j|--jars)
    REGISTERED_JARS="$2"
    shift # past argument
    ;;
    -b|--bpath)
    BUILD_JAR_PATH="$2"
    shift # past argument
    ;;
    -n|--njar)
    BUILD_JAR_NAME="$2"
    shift # past argument
    ;;
    *)
    echo " # unknown option"
    ;;
esac
shift # past argument or value
done

info "[INPUT_PATH] ${INPUT_PATH}"
info "[OUTPUT_PATH] ${OUTPUT_PATH}"
info "[REGISTERED_JARS] ${REGISTERED_JARS}"
info "[BUILD_JAR_PATH] ${BUILD_JAR_PATH}"
info "[BUILD_JAR_NAME] ${BUILD_JAR_NAME}"


info "--------------COMPLETED PARSING ARGUMENTS------------------"

export BASE_LOG_DIR;
export MAPREDUCE_QUEUE_NAME;
export PIG_UDF_IMPORT_LIST="org.apache.pig.piggybank.storage.avro.AvroStorage:org.apache.pig.piggybank.storage.CSVExcelStorage:com.airline.project.pigudf.UUIDGenerator";
export EXTERNAL_JARS=${REGISTERED_JARS}:${BUILD_JAR_PATH}/${BUILD_JAR_NAME}
export JOB_NAME=pig_raw_load_${TABLE_NAME}

info "-----------------CHECKING FOR THE LOG DIR------------------"

createLogDirIfNotExists "${TABLE_NAME}"

export TABLE_NAME;

####################Running pig script of ups###################
info "----------RUNNING PIG SCRIPT FOR ${TABLE_NAME}-------------"

info "--------------------COMMAND EXECUTED-----------------------"
info "bash $PIG_HOME/airline.sh"
info "-q default -t airline -i /user/cloudera/testAirline/data/airline.csv"
info "-b /home/cloudera/Desktop/Spark-Kafka-UseCase -n BigData_Projects-1.0-SNAPSHOT_java7.jar "
info "-o /user/cloudera/testAirline/outdata/airline_test "
info "-j /home/cloudera/Desktop/Spark-Kafka-UseCase/Jar_Pig_Avro/avro.jar:/home/cloudera/Desktop/Spark-Kafka-UseCase/Jar_Pig_Avro/json-simple-1.1.jar:/home/cloudera/Desktop/Spark-Kafka-UseCase/Jar_Pig_Avro/piggybank.jar "
info "------------------------------------------------------------"

info "executePig -useHCatalog  -param INPUT_PATH=${INPUT_PATH} -param OUTPUT_PATH=${OUTPUT_PATH} -f ${PIG_HOME}/airline.pig "


executePig -useHCatalog  -param INPUT_PATH=${INPUT_PATH} -param OUTPUT_PATH=${OUTPUT_PATH} -f ${PIG_HOME}/airline.pig

info "-----------------VALIDATING THE EXIT STATUS-----------------"
if [ $? -eq 0 ]; then
		info "Pig Script for the Raw Load for the ${TABLE_NAME} has been completed successfully!!!!"
fi

info "-----------------END PIG JOB FOR TABLE $TABLE_NAME-----------------"




