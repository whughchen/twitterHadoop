twitterHadoop
=============

modules
-----
1. SourceAnalysis, statistics on which channel did those twitter come from.
2. UserAnalysis,   analysis the most Active user, the most Popular user, etc. (top 10 ,etc.)
3. TopicAnalysis,  Statistics on what's the most hot topic going on the twitter.(top 10,etc.)


Steps to run the executable package:
------------
1. Follow the struction on https://github.com/nathanmarz/storm/wiki/Setting-up-a-Storm-cluster to set up a storm cluster;
2. Configure your execution environment: copy the the jar files specified in the .project(in this folder) file to your $STORM_HOME/lib;
3. run command:


----
    storm jar target/twitterStreamAnalysis-0.0.1-SNAPSHOT.jar TwttrStrmAnlyst.StreamAnalysisTopo twitterStream
----


   this program will download twitter messages form Twitter stream API, and save to this folder named "YYYY-MM-DD-HH", means that it will generate each file for each file.
   
4. At the same time , this program will generation 2-minute intervaled "mostActive","mostPopular" user statistics in this folder.


Additional:
---------
  (1) If your country or district was blocked to access facebook, twitter, youtobu and other web medias, you need to follow the instructions  to circumvention the firewall of your ISP provider.
       (It made me a lot trouble since twitter was offically blocked by Chinese government for political reasons .)
  (2) A real time running storm cluster control panel can be reviewed from: http://54.248.240.232:8080/
  (3) A visualized view of the statistics can be found : http://210.75.252.106:8888/twitterAnalysis/


