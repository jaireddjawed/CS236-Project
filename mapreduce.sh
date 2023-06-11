# create alias commands
alias hadoop='/usr/local/cellar/hadoop/$HADOOP_VERSION/libexec/bin/hadoop'
alias hdfs='/usr/local/cellar/hadoop/$HADOOP_VERSION/libexec/bin/hdfs'
alias yarn='/usr/local/cellar/hadoop/$HADOOP_VERSION/libexec/bin/yarn'

mkdir -p classes
javac MapReduce.java -cp $(hadoop classpath) -d classes
jar -cvf MapReduce.jar -C classes/ .

# remove input directory from hdfs and replace it
hdfs dfs -rm -r /inputdata
hadoop fs -put ./inputs /inputdata

# remove output directory from hdfs
hdfs dfs -rm -r /outputdata

# run the WordCount job
hadoop jar MapReduce.jar MapReduce /inputdata/*.csv /outputdata

# print the output of the WordCount job
hdfs dfs -cat /outputdata/part-r-00000
