# Project_On_Spark_Scala_Cassandra

Tried to develop a real life project in Spark using Scala.


brief details about the project :: 

imported a text datafile into spark dataframe and on that have done some aggregation and manipulation, which finally 
have written into Cassandra tables. All these using Scala language.

--make sure to modify your pom.xml file.

--there are input parameters you need to have to provide, like the path to the downloaded adult.data file in your local.

Details of the project ::

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
   
3) when you are wrting data into Cassandra table, the data has to be in RDD not in DataFrame. If you convert RDD to DF and 
   back & forth, you might get error writing into cassandra table using "saveToCassandra". So that is why I have added these
   
     implicit val rowWriter : RowWriterFactory[Row] = SqlRowWriter.Factory
     
   if you have this in your code, even if you have converted your DF to RDD, you would not be getting any error, because scala
   takes care of the conversion implicitely.
