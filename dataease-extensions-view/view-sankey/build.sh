#!/bin/sh
mvn clean package

cp view-sankey-backend/target/view-sankey-backend-1.18.3.jar .

zip -r sankey.zip  ./view-sankey-backend-1.18.3.jar ./plugin.json

rm -f ./view-sankey-backend-1.18.3.jar
