hdfs dfs -mkdir input_r
hdfs dfs -put dataset input_r

Execution 
--------------------------------------------------------------
hadoop jar assign1.jar bigdata.hadoop.assignment1.A1Ques1 input_r/users.dat output_r1
hadoop jar assign1.jar bigdata.hadoop.assignment1.A1Ques2 input_r/users.dat output_r2
hadoop jar assign1.jar bigdata.hadoop.assignment1.A1Ques3 input_r/movies.dat output_r3

View the output
--------------------------------------------------------------
hdfs dfs -cat output_r1/*
hdfs dfs -cat output_r2/*
hdfs dfs -cat output_r3/*