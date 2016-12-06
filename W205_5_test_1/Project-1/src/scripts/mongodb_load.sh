#!/bin/sh

#echo 'Importing files one by one\n'

#echo "pwd:"$(pwd)
#current_dir = $(pwd)
NOW=$(date +"%Y-%m-%d")
echo $NOW
LOGFILE="log-$NOW.log"
cd /data/src/scripts
#current_dir = $(pwd)

echo "current directory: "$(pwd)

#python tweets_ld_iphone.py

echo 'Sleeping 10 seconds'
#sleep 10

echo 'Importing files one by one'

string1=twitter_
string2=_db
string3=_collection

for json in $(cat file_list.txt); do
  product=$(echo $json| cut -d"_" -f 1)
  echo "Product Name : "$product
  echo "Start loading file:$json" 

  
  mongoimport --db $string1$product$string2 --collection $string1$product$string3 --type json --file /data/rawdata/$product/$json
  echo $string1$product$string2
  echo $string1$product$string3
  echo 'Sleeping 10 seconds.'
  #sleep 10
  echo "$json loaded."

done


