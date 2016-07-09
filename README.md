# Spark-GeoSpatial-Library_JAVA

A JAVA library using Apache Spark and Hadoop on distributed clusters to run geometric
operations like geometric union, spatial join, and closest pair on large dataset
and analyzed the performance.

Closest Pair:
arg0    : "hdfs://master:54310/content/closestpair.csv"    (Location of input file)
arg1    : "hdfs://master:54310/content/closestpairoutput"  (Location of output file)
example : ~$ spark-submit --class com.jeniljain.sparkExample.closestPair --jars /home/hduser/Downloads/jts-1.13.jar --master spark://192.168.0.6:7077  /home/hduser/Downloads/closestPair-0.1.jar "hdfs://master:54310/content/closestpair.csv" "hdfs://master:54310/content/closestpairoutput"

Polygon Union:
arg0    : "hdfs://master:54310/content/PolygonUnionTestData.csv"    (Location of input file)
arg1    : "hdfs://master:54310/content/PolygonUnionoutput"  (Location of output file)
example : ~$ spark-submit --class com.jeniljain.sparkExample.polygonUnion --jars /home/hduser/Downloads/jts-1.13.jar --master spark://192.168.0.6:7077  /home/hduser/Downloads/union-0.1.jar "hdfs://master:54310/content/PolygonUnionTestData.csv" "hdfs://master:54310/content/PolygonUnionoutput"
