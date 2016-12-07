#!/usr/bin/env mongo

#dir1=$'data/W205_5_group_allan_eric_praj/Project-1'
dir1=$'data/w205_test_1/W205_5_test_1/Project-1'

echo "dir1 :"$dir1

NOW=$(date +"%Y-%m-%d")
echo $NOW
LOGFILE="log-$NOW.log"

#cd /data/W205_5_group_allan_eric_praj/Project-1/data/
cd /$dir1/data/

echo $(pwd)

chmod 777 /$dir1/data/rawdata/
chmod 777 /$dir1/src/scripts/
chmod 777 /$dir1/data/analysis_output/


cd /$dir1/src/scripts/

string1=twitter_
string2=_db
string3=_collection

for json in $(cat file_list.txt); do
  product=$(echo $json| cut -d"_" -f 1)
  echo "Product Name : "$product

  var db = new Mongo().getDB($string1$product$string2);
  db.dropDatabase();
  var col = new Mongo().getCollection($string1$product$string3);
  db.$string1$product$string3.drop();

done
