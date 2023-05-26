# Greenplum Loader

Загрузчик данных из Greenplum в Oracle. Для чтения данных с Greenplum доступны коннекторы - pivotal или jdbc. Документация по pivotal коннектору доступна по ссылке: https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-overview.html

Параметры для запуска загрузчика:

| Attribute | Desctiption |
| ------ | ------ |
| connector_type | jdbc or pivotal |
| GP_table       | Table for reading from Greenplum. Format <schema_name.table_name> |
| GP_connection_string | Greenplum connection string |
| GP_login | User to create a connection to Greenplum |
| GP_provider_path | Path to credential container (jceks:/hdfs//user/tech_datalake/jceks/<container_name>.jceks") |
| GP_password_alias | Alias of credential container |
| GP_partition_column | The name of the Greenplum Database table column to use for Spark partitioning. The default partition column is the internal Greenplum Database table column named gp_segment_id |
| GP_pool_size | The maximum number of connections in the connection pool. Optional, the default value is 64 |
| GP_pool_timeoutMs | The amount of time, in milliseconds, after which an inactive connection is considered idle.Optional, the default value is 10,000 (10 seconds). |
| Spark_num_partitions | The number of Spark partitions. Optional, and valid to specify only when the partitionColumn is not gp_segment_id. |
| write_mode |  Mode of write dataframe to Oracle. Available: Overwrite/Append/Ignore/ErrorIfExists |
| ORA_target_table | Target table in Oracle |
| ORA_connection_string | Connection string Oracle |
| ORA_login |  User to create a connection to Oracle |
| ORA_provider_path | Path to credential container (jceks:/hdfs//user/tech_datalake/jceks/<container_name>.jceks") |
| ORA_password_alias | Alias of credential container |
| ORA_partition_column | Partition column in Oracle |
| ORA_num_partition | The maximum number of partitions that can be used for parallelism in table writing |

  
Пример запуска Spark приложения для чтения данных из представления v_account_sdim_spark в таблицу на Oracle Sandbox:
```
spark-submit \
--master yarn \
--deploy-mode cluster \
--executor-cores 1 \
--driver-memory 12G \
--driver-cores 10 \
--executor-memory 8G \
--class Greenplum2OracleLoader \
--queue root.service \
--name greenplum2oracle_view_account_sdim \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.kerberos.keytab=/home/tech_datalake/tech_datalake.keytab \
--conf spark.kerberos.principal=tech_datalake \
--jars "hdfs:///user/tech_datalake/ojdbc6.jar,/opt/cloudera/parcels/CDH/lib/spark/jars/postgresql-42.5.1.jar,hdfs:///user/tech_oozie/libs/greenplum-connector-apache-spark-scala_2.11-2.1.4.jar" \
--conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/spark/jars/postgresql-42.5.1.jar,hdfs:///user/tech_oozie/libs/greenplum-connector-apache-spark-scala_2.11-2.1.4.jar" \
--conf "spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/lib/spark/jars/postgresql-42.5.1.jar,hdfs:///user/tech_oozie/libs/greenplum-connector-apache-spark-scala_2.11-2.1.4.jar" \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.executor.memoryOverhead=2g \
--conf spark.dynamicAllocation.minExecutors=70 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.initialExecutors=90 \
--conf spark.dynamicAllocation.executorIdleTimeout=360s \
--conf spark.default.parallelism=100 \
--conf spark.shuffle.service.enabled=true \
hdfs:///user/tech_oozie/libs/greenplum-oracle-loader-1.0-SNAPSHOT.jar \
"pivotal" \
"public.v_account_sdim_spark" \
"jdbc:postgresql://ydbgpprod:5432/ycloud_db1" \
"tech_hadoop" \
"jceks://hdfs/user/tech_datalake/jceks/ycloud_db_prod.tech_hadoop.jceks" \
"ycloud_db_prod.tech_hadoop.alias" \
"job_insert" \
18 \
"N" \
"N" \
"overwrite" \
"tech_gp_load.v_account_sdim_spark" \
"jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=exa6-scan1)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=exa2-scan)(PORT=1521)))(CONNECT_DATA=(SERVER=dedicated)(SERVICE_NAME=SANDBOX_TAF)))" \
"TECH_GP_LOAD" \
"jceks://hdfs/user/tech_datalake/jceks/sandbox_taf_tech_gp_load.jceks" \
"sandbox_taf_tech_gp_load.alias" \
"N" \
90
```
