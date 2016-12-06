#!/bin/bash

DIR=$1
REGEX=$2

for file in $DIR
outfile = $file".parsed"
do
    #if its a .json file
	if [[ $file =~ \.json$ ]]; then
		#check if the line contains regex
		grep '"'${REGEX}'"' -i $file > $outfile 
    fi
done

#merge all .parsed
cat *.parsed > "merged.json"

