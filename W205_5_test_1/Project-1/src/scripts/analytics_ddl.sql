DROP DATABASE IF EXISTS product_analytics CASCADE;

CREATE DATABASE IF NOT EXISTS product_analytics LOCATION 'hdfs://localhost:8020/user/hive/warehouse/analyticsdb/';

CREATE  EXTERNAL TABLE IF NOT EXISTS product_analytics.clean_tweet_dtl
(
  `created_at` string ,
  `favorite_count` string ,
  `id` string ,
  `lang` string ,
  `possibly_sensitive` string ,
  `retweet_count` string ,
  `retweeted` string ,
  `text` string ,
  `user_created_at` string ,
  `user_favourites_count` string,
  `user_followers_count` string ,
  `user_friends_count` string ,
  `user_id` string ,
  `user_listed_count` string ,
  `user_screen_name` string ,
  `user_statuses_count` string ,
  `user_time_zone` string ,
  `place_country` string ,
  `place_country_code` string ,
  `place_id` string ,
  `place_place_type` string ,
  `place_city` string ,
  `place_state` string ,
  `geo_latitude` string ,
  `geo_longitude` string ,
  `hashtags` string ,
  `ss_compound` string ,
  `ss_pos` string ,
  `ss_neg` string ,
  `ss_neu` string ,
  `brand` string
)   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar"="\t", "quoteChar"='"', "escapeChar" = '\\')
LOCATION 'hdfs://localhost:8020/user/w205/product_analytics_files';
LOAD DATA INPATH '/user/w205/analysis_data/google/google_result.tsv' INTO TABLE product_analytics.clean_tweet_dtl;
LOAD DATA INPATH '/user/w205/analysis_data/apple/apple_result.tsv' INTO TABLE product_analytics.clean_tweet_dtl;
LOAD DATA INPATH '/user/w205/analysis_data/samsung/samsung_result.tsv' INTO TABLE product_analytics.clean_tweet_dtl;

DROP TABLE IF EXISTS product_analytics.features PURGE;

CREATE  EXTERNAL TABLE IF NOT EXISTS product_analytics.features
(
  `feature` string 
 )   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar"="\t", "quoteChar"='"', "escapeChar" = '\\')
LOCATION 'hdfs://localhost:8020/user/w205/product_features';

LOAD DATA INPATH '/user/w205/analysis_data/features/features.txt' INTO TABLE product_analytics.features;


DROP TABLE IF EXISTS product_analytics.sentiment_check PURGE; 

CREATE TABLE IF NOT EXISTS product_analytics.sentiment_check 
AS SELECT 
   CAST(concat(regexp_extract(`created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 3)
         ,case regexp_extract(`created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 1)
            when 'Jan' then '-01-'
            when 'Feb' then '-02-'
            when 'Mar' then '-03-'
            when 'Apr' then '-04-'
            when 'May' then '-05-'
            when 'Jun' then '-06-'
            when 'Jul' then '-07-'
            when 'Aug' then '-08-'
            when 'Sep' then '-09-'
            when 'Oct' then '-10-'
            when 'Nov' then '-11-'
            when 'Dec' then '-12-'
            else '-**-'
          end
         ,regexp_extract(`created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 2)
         ) as date) create_at_date,
  regexp_extract(`created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 3) as create_at_year,
  regexp_extract(`created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 1) as create_at_mon,
  regexp_extract(`created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 2) as create_at_dd,
  cast(`retweet_count`as int) `retweet_count`, 
  `text` , 
  `user_id` , 
  `place_id`,
  `user_followers_count`, 
  `place_country` , 
  `place_country_code` , 
  `place_city` , 
  `place_state` , 
   cast(`ss_compound` as decimal(18,6))`ss_compound` , 
   cast(`ss_pos` as decimal(18,6)) `ss_pos` , 
   cast(`ss_neg` as decimal(18,6)) `ss_neg`, 
   cast(`ss_neu` as decimal(18,6)) `ss_neu`, 
  `brand`
 FROM product_analytics.clean_tweet_dtl;


DROP TABLE IF EXISTS product_analytics.users PURGE; 


CREATE TABLE IF NOT EXISTS product_analytics.users 
AS SELECT DISTINCT
     CAST(concat(regexp_extract(`user_created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 3)
         ,case regexp_extract(`user_created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 1)
            when 'Jan' then '-01-'
            when 'Feb' then '-02-'
            when 'Mar' then '-03-'
            when 'Apr' then '-04-'
            when 'May' then '-05-'
            when 'Jun' then '-06-'
            when 'Jul' then '-07-'
            when 'Aug' then '-08-'
            when 'Sep' then '-09-'
            when 'Oct' then '-10-'
            when 'Nov' then '-11-'
            when 'Dec' then '-12-'
            else '-**-'
          end
         ,regexp_extract(`user_created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 2)
         ) as date) user_create_at_date,
  regexp_extract(`user_created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 3) as user_create_at_year,
  regexp_extract(`user_created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 1) as user_create_at_mon,
  regexp_extract(`user_created_at`, '^[A-Z][a-z]* ([A-Z][a-z]*) ([0-9][0-9]*) .* ([0-9]*)$', 2) as user_create_at_dd,
  `user_created_at` ,
  cast(`user_favourites_count` as int) `user_favourites_count` ,
  cast(`user_followers_count` as int) `user_followers_count` ,
  cast(`user_friends_count` as int) `user_friends_count` ,
  `user_id`  ,
  cast(`user_listed_count` as int) `user_listed_count` ,
  `user_screen_name` ,
  cast(`user_statuses_count` as int) `user_statuses_count` ,
  `user_time_zone` ,
  `place_id` 
 FROM product_analytics.clean_tweet_dtl;

DROP TABLE IF EXISTS product_analytics.location PURGE; 

CREATE TABLE IF NOT EXISTS product_analytics.location 
AS SELECT DISTINCT 
  `place_country` , 
  `place_country_code` , 
  `place_id` , 
  `place_place_type` , 
  `place_city` , 
  `place_state` ,
  `geo_latitude` , 
  `geo_longitude`  
FROM product_analytics.clean_tweet_dtl;

DROP TABLE IF EXISTS product_analytics.wordcloud PURGE; 

CREATE TABLE IF NOT EXISTS product_analytics.wordcloud
AS SELECT brand, word, COUNT(*) word_count 
   FROM product_analytics.clean_tweet_dtl LATERAL VIEW explode(split(text, ' ')) lTable as word
   GROUP BY brand, word ;
  

