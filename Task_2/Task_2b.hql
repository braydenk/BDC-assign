DROP TABLE twitterdata;
DROP TABLE topHashtag

-- Create a table for the input data
CREATE TABLE twitterdata (tokenType STRING, month STRING, count BIGINT,
  hashtagName STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/twitter.tsv' INTO TABLE twitterdata;

-- Solution (Note: this is throwing an error??)
CREATE TABLE topHashtag
SELECT sum(count) as totalTweets, hashtagName
FROM twitterdata
GROUP BY hashtagName
ORDER BY totalTweets desc
limit 1;

-- Format and insert into textfile
INSERT OVERWRITE LOCAL DIRECTORY './Task_2b-out/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
  SELECT * FROM topHashtag;



