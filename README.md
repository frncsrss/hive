Personal hive UDAFs

There are a bunch of Hive UDAFs (User-Defined Aggregate Functions) that are not in the standard Hive distribution as they could potentially yields to OOM on large datasets.

To use them, you need to load the jar file and then create a temporary function for each function you want to use:

ADD JAR target/hive-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION string_builder AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStringBuilder';
CREATE TEMPORARY FUNCTION to_list AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFToList';
CREATE TEMPORARY FUNCTION to_map AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFToMap';
CREATE TEMPORARY FUNCTION counter_map AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCounterMap';
...

They are pretty straightforward. They create a list, string or counter map by aggregating a column or a map from two columns.

Note that string_builder(col1) is equivalent to concat_ws('', to_list(cast(col1 AS STRING))) except that since the last one is combining a UDF and a UDAF, you cannot reference the column alias in an HAVING clause for the latter (possible for the former) and thus, you need to repeat it. However, you can choose the separator, something I don't allow with the StringBuilder.

For example:
FROM table
SELECT col1, string_builder(col2) AS sb
GROUP BY col1"
HAVING sb in (...)

but

FROM table
SELECT col1, concat_ws('', to_list(cast(col2 AS STRING))) AS path
GROUP BY col1"
HAVING concat_ws('', to_list(cast(col2 AS STRING))) in (...)
