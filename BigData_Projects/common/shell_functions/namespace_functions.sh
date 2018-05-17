#!/usr/bin/env bash

#Funtion to get the parameter file name ie: uat/dev/prod.
function getNamespaceFileForCurrentUser () {

	local CURRENT_USER=`echo $USER`;
	if [ $CURRENT_USER == "cloudera" ]; then
	    echo "namespace.dev.properties";
	elif [ $CURRENT_USER == "cloudera_prod" ]; then
	    echo "namespace.prod.properties";
	else
	    echo "Parameter file is not defined for the user ${CURRENT_USER}. Please check!!";
    fi

}