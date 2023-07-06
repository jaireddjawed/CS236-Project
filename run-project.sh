alias hadoop='/usr/local/cellar/hadoop/$HADOOP_VERSION/libexec/bin/hadoop'
alias hdfs='/usr/local/cellar/hadoop/$HADOOP_VERSION/libexec/bin/hdfs'
alias yarn='/usr/local/cellar/hadoop/$HADOOP_VERSION/libexec/bin/yarn'

# javac JoinDatasets.java -d classes

# mkdir -p $2
# for file in $1/*; do
#     java -cp classes JoinDatasets $file $2
# done

# remove input directory from hdfs and replace it
hdfs dfs -rm -r /inputdata

# upload combined input folder to hdfs
javac MapReduce.java -cp $(hadoop classpath) -d classes
jar -cvf MapReduce.jar -C classes/ .

hadoop fs -put $2 /inputdata

# remove output directory from hdfs
hdfs dfs -rm -r /outputdata

# run the WordCount job
hadoop jar MapReduce.jar MapReduce /inputdata/*.csv /outputdata

# print the output of the WordCount job
hdfs dfs -cat /outputdata/part-r-00000

# copy output to local file system
mkdir -p $3
hadoop fs -get /outputdata/part-r-00000 $3/toprevenuemonth.txt
