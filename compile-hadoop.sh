# create alias commands
alias hadoop='/usr/local/cellar/hadoop/$HADOOP_VERSION/libexec/bin/hadoop'
alias hdfs='/usr/local/cellar/hadoop/$HADOOP_VERSION/libexec/bin/hdfs'
alias yarn='/usr/local/cellar/hadoop/$HADOOP_VERSION/libexec/bin/yarn'

mkdir -p classes
javac WordCount.java -cp $(hadoop classpath) -d classes
jar -cvf WordCount.jar -C classes/ .

# remove input directory from hdfs and replace it
hdfs dfs -rm -r /inputdata
hadoop fs -put ./inputs /inputdata

# remove output directory from hdfs
hdfs dfs -rm -r /outputdata

# run the WordCount job
hadoop jar WordCount.jar org.myorg.WordCount /inputdata/*.csv /outputdata

# print the output of the WordCount job
hdfs dfs -cat /outputdata/part-00000
