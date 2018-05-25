In application.properties set your own twitter api credentials: 

twitter4j.oauth.consumerKey=

twitter4j.oauth.consumerSecret= 

twitter4j.oauth.accessToken=

twitter4j.oauth.accessTokenSecret=

For packaging : sbt clean assembly and get the package in target

To pass args to the program we use scopt. Scopt is a little command line options parsing library.

* --filters: filters predicates for tweets
* --checkpointDirectory: checkpoint directory in order to recovery states from failures
* --batchDuration: batch interval
* --mongoOutputUri: MongoDB uri for writing output data

Usage in local :

* launch an instance of MongoDB
* sbt clean assembly
* spark-submit --class=com.zengularity.twitter.Twitter spark-streaming-twitter.jar --filters RGPD --checkpointDirectory ./checkpoint --batchDuration 10 --mongoOutputUri mongodb://127.0.0.1/test.metriccount