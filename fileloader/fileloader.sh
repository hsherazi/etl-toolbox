#!/bin/bash
# java -Dlog4j.configurationFile="./log4j2.json" -cp "./*" com.osrdata.etltoolbox.fileloader.Main ${1+"$@"}
java -jar fileloader.jar ${1+"$@"}
