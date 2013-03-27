#twitterHadoop
=============

###Modules
-----
1. SourceAnalysis, statistics on which channel did those twitter come from.
2. userAnalysis,   analysis the most Active user, the most Popular user, etc. (top 10 ,etc.)
3. TopicAnalysis,  Statistics on what's the most hot topic going on the twitter.(top 10,etc.)


###Steps to run the executable package:
------------
1. Set up a Hadoop cluster;
2. Configure your execution environment: add jar files specified in the .project(in this folder) file to your $Hadoop_HOME/lib;
3. put the source twitter data, eg: 2013-03-25-18 in any folder of this directory such as "examples".
3. run command:

 (1)**to  analysis which channel did those twitter come from**.
  
----
    hadoop fs -mkdir in
    hadoop fs -put examples/2013-03-25-18  in
    hadoop jar examples/twitterHadoop.jar  sourceAnalysis.DeviceStatistic   in DeviceStatistic 
    hadoop fs -get DeviceStatistic   .
    cat  DeviceStatistic/part*
----

   
 (2) **to analysis the most hot topic happened on twitter during this time**.  
  
----
     hadoop jar examples/twitterHadoop.jar  topicAnalysis.mostHotTopic   in mostHotTopic 
     hadoop fs -get  mostHotTopic    .
----
  
 (3) **to analysis the most active user on twitter during this time**. (sort by descending) 
  
----
     hadoop jar examples/twitterHadoop.jar  userAnalysis.mostActive   in mostActive
     hadoop fs -get  mostActive    .
----
   
  (4) **to analysis the most popular user on twitter during this time**.   (sort by descending) 
   
----
     hadoop jar examples/twitterHadoop.jar  userAnalysis.mostPopular   in mostPopular
     hadoop fs -get  mostPopular    .
----
   

    

###Additional:
---------
  1. If your country or district was blocked to access facebook, twitter, youtobu and other web medias, you need to follow the instructions  to circumvention the firewall of your ISP provider.
       (It made me a lot trouble since **twitter was offically blocked by Chinese government for political reasons**.)
  2. A real time running storm cluster control panel can be reviewed from: http://54.248.240.232:8080/
  3. A visualized view of the statistics can be found : http://210.75.252.106:8888/twitterAnalysis/


