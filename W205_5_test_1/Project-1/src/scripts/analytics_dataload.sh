#!/bin/bash

# Create directory in HDFS and put files into hdfs

NOW=$(date +"%Y-%m-%d")
echo $NOW

cd /data/src/scripts
echo $(pwd)

echo 'Make directory structures for all hdfs files.'
hdfs dfs -mkdir -p /user/w205/product_features/
hdfs dfs -mkdir -p /user/w205/analysis_data/apple/
hdfs dfs -mkdir -p /user/w205/analysis_data/samsung/
hdfs dfs -mkdir -p /user/w205/analysis_data/google/
hdfs dfs -mkdir -p /user/w205/analysis_data/features/
sleep 5
echo 'Directory structures completed.'


echo 'Removing files temporary files.'
rm -r /data/analysis_output/apple_result_temp.tsv
rm -r /data/analysis_output/google_result_temp.tsv
rm -r /data/analysis_output/samsung_result_temp.tsv


sleep 5

echo 'Temporary files removed..'

echo 'Remove hive table location files.'

hadoop fs -rm /user/w205/product_analytics_files/*.tsv
hadoop fs -rm /user/w205/analysis_data/features/features.txt
sleep 5

echo 'Removed hive table location files..'

echo 'Copy clean analysis files after removing headers to as temporary files.'

tail -n +2 /data/analysis_output/apple_result.tsv > /data/analysis_output/apple_result_temp.tsv
tail -n +2 /data/analysis_output/google_result.tsv > /data/analysis_output/google_result_temp.tsv
tail -n +2 /data/analysis_output/samsung_result.tsv > /data/analysis_output/samsung_result_temp.tsv

sleep 5

echo 'Temporary files created.'

echo 'Remove hadoop files before load.'

hdfs dfs -rm /user/w205/analysis_data/apple/apple_result.tsv
hdfs dfs -rm /user/w205/analysis_data/samsung/samsung_result.tsv
hdfs dfs -rm /user/w205/analysis_data/google/google_result.tsv

sleep 5

echo 'Hadoop files removed.'

echo 'Copy clean analysis files to hdfs.'

hdfs dfs -put /data/analysis_output/apple_result_temp.tsv /user/w205/analysis_data/apple/apple_result.tsv
hdfs dfs -put /data/analysis_output/samsung_result_temp.tsv /user/w205/analysis_data/samsung/samsung_result.tsv
hdfs dfs -put /data/analysis_output/google_result_temp.tsv /user/w205/analysis_data/google/google_result.tsv
hdfs dfs -put /data/analysis_output/features.txt /user/w205/analysis_data/features/features.txt

sleep 5

echo 'Files moved to final destination.'

cd /data/src/scripts/

echo $(pwd)

hive -f $(pwd)/analytics_ddl.sql


