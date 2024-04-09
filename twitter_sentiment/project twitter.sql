-- This first function is made to import table
CREATE EXTERNAL TABLE IF NOT EXISTS `twitter_project`.`elon_musk_twitter` (
  `tweet_id` float,
  `name` string,
  `username` string,
  `tweet` string,
  `followers` integer,
  `location` string,
  `time` string,
  `is_retweet` boolean,
  `orig_tweet` string,
  `mentions` string,
  `tweet_cleaned` string,
  `sentiment_score` double,
  `filtered` string,
  `label` double,
  `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = '\t',
  'collection.delim' = '\u0002',
  'mapkey.delim' = '\u0003'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://b18-kevin2/Project/predictions2/predictions2.csv/';

# This table was created and mostly used throughout. Most of it is relabelling things.
CREATE TABLE elon_musk_edit AS
SELECT *,
        CASE
            WHEN "prediction" = 0 THEN 'Terrible'
            WHEN "prediction" = 1 THEN 'Poor'
            WHEN "prediction" = 2 THEN 'Neutral'
            WHEN "prediction" = 3 THEN 'Good'
            WHEN "prediction" = 4 THEN 'Excellent'
            ELSE 'Unknown'
        END AS "prediction_lab",
        CASE
            WHEN "is_retweet" = false THEN 'Tweet'
            WHEN "is_retweet" = true THEN 'Retweet'
            ELSE 'Unknown'
        END AS "tweet_retweet",
        CASE
            WHEN length("time") > 10
                THEN CAST(REPLACE(SUBSTRING("time", 1, 19), 'T', ' ') AS timestamp) # This was made to convert time
            ELSE NULL
        END AS "time_clean",
        CASE
            WHEN "prediction" = 0 OR "prediction" = 1 THEN 'Negative'
            WHEN "prediction" = 3 OR "prediction" = 4 THEN 'Positive'
            WHEN "prediction" = 2 THEN 'Neutral'
            ELSE 'Unknown'
        END AS "prediction_simp"
FROM "elon_musk_twitter";

# This table was created so I can edit the word cloud. This makes a new row for each word in the filtered column.
CREATE TABLE word_thing AS
SELECT *
FROM "elon_musk_edit"
CROSS JOIN UNNEST(SPLIT("filtered", ',')) as t(words)