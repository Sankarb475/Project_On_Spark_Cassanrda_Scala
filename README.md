# Project_On_Spark_Scala_Cassandra

Tried to develop a real life project in Spark using Scala.

have downloaded a datafile from ==> 
(UCI Machine Learning Adult dataset) => https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data

Imported the file data as dataframe and have done some basic manipulation on that using Scala and finally writing those
dataframes into cassandra table.

This would be a very good example and use case if you are looking to play with Spark using Scala in real life.

Please do get in touch with me :: sankarb475@gmail.com if you have any issue implementing it.


Note : 
1) cross check whether the data in dataframe which will be written to cassandra, the primary key column values are unique in
   dataframe.
   
2) Cassandra internally changes all the keyspace/table/columnname to lower case, make sure when you are working on the
   dataframes, you give the proper names.
