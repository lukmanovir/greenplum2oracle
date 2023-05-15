import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object Greenplum2OracleLoader extends App {

  val connector_type = args(0) // Only jdbc or pivotal
  val GP_table = args(1) // Table for reading from Greenplum. Format schema.table_name
  val GP_connection_string = args(2) // Greenplum connection string
  val GP_login = args(3) // User to create a connection to Greenplum
  val GP_provider_path = args(4) // Path to credential container (jceks:/hdfs//user/tech_datalake/jceks/<container_name>.jceks"
  val GP_password_alias = args(5) // Alias of credential container
  val GP_partition_column = args(6) /* The name of the Greenplum Database table column to use for Spark partitioning.
  The default partition column is the internal Greenplum Database table column named gp_segment_id */
  val GP_pool_size = args(7) // The maximum number of connections in the connection pool. Optional, the default value is 64.
  val GP_pool_timeoutMs = args(8) /* The amount of time, in milliseconds, after which an inactive connection is considered idle.
  Optional, the default value is 10,000 (10 seconds). */
  val Spark_num_partitions = args(9) // The number of Spark partitions. Optional, and valid to specify only when the partitionColumn is not gp_segment_id.
  val write_mode = args(10) // Mode of write dataframe to destination. Overwrite/Append/Ignore/ErrorIfExists
  val ORA_target_table = args(11) // Target table in Oracle
  val ORA_connection_string = args(12) // Connection string Oracle
  val ORA_login = args(13) // User to create a connection to Oracle
  val ORA_provider_path = args(14) // Path to credential container (jceks:/hdfs//user/tech_datalake/jceks/<container_name>.jceks"
  val ORA_password_alias = args(15) // Alias of credential container
  val ORA_partition_column = args(16) // Partition column in Oracle
  val ORA_num_partition = args(17) // The maximum number of partitions that can be used for parallelism in table writing

  var sparkSession = SparkSession.builder()
    .appName("Greenplum2OracleLoadingJob")
    .enableHiveSupport()
    .getOrCreate()

  //sparkSession.sparkContext.setLogLevel("ALL")

  def readGreenplumData(): Unit = {
    println("Read data from Greenplum started...")
    val startTime = System.nanoTime()
    val dbHadoopConf = new Configuration()
    dbHadoopConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, GP_provider_path)

    val dbCredentials = new Properties()
    dbCredentials.put("user", GP_login)
    dbCredentials.put("password", dbHadoopConf.getPassword(GP_password_alias).mkString)

    if (connector_type.toLowerCase() == "jdbc") {
      val sourceDF = sparkSession.read
        .option("driver", "org.postgresql.Driver")
        .jdbc(GP_connection_string,s"$GP_table", dbCredentials)
      val duration = (System.nanoTime() - startTime) / 1e9d
      println("Read finished.")
      println(s"Elapsed time for reading from Greenplum (in seconds): $duration")
      writeOracleData(sourceDF)
    }
    else if (connector_type.toLowerCase() == "pivotal")
    {
      var gpOptionMap = Map(
        "url" -> GP_connection_string,
        "user" -> GP_login,
        "password" -> dbHadoopConf.getPassword(GP_password_alias).mkString,
        "dbschema" -> s"${GP_table.split("\\.")(0)}",
        "dbtable" -> s"${GP_table.split("\\.")(1)}",
        "server.useHostname"-> "true"
      )
      if (GP_pool_timeoutMs != "N") {
        gpOptionMap += ("pool.timeoutMs" -> GP_pool_timeoutMs)
      }
      if (GP_pool_size != "N") {
        gpOptionMap += ("pool.maxSize" -> GP_pool_size)
      }
      if (GP_partition_column != "N") {
        gpOptionMap += ("partitionColumn" -> GP_partition_column)
        // valid to specify only when the partitionColumn is not gp_segment_id. The default value is the number of primary segments in the Greenplum Database cluster.
        if (GP_partition_column != "gp_segment_id" && Spark_num_partitions != "N") {
          gpOptionMap += ("partitions" -> Spark_num_partitions)
        }
      }
      else if (Spark_num_partitions != "N"){
        gpOptionMap += ("partitions" -> Spark_num_partitions)
      }

      val sourceDS = sparkSession.read
        .format("greenplum")
        .options(gpOptionMap)
        .load()

      val duration = (System.nanoTime() - startTime) / 1e9d
      println(s"Elapsed time for reading from Greenplum (in seconds): $duration")
      writeOracleData(sourceDS)
    }
  }

  def writeOracleData(dataframe: DataFrame): Unit = {
    println("Write data to Oracle started...")

    val OradbHadoopConf = new Configuration()
    OradbHadoopConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, ORA_provider_path)

    val OraDBCredentials = new Properties()
    OraDBCredentials.put("user", ORA_login)
    OraDBCredentials.put("password", OradbHadoopConf.getPassword(ORA_password_alias).mkString)

    val startTime =  System.nanoTime()
    if (ORA_partition_column == "N") {
      println(s"ORA_partition_column: $ORA_partition_column")
      dataframe.write
        .mode(write_mode)
        .option("numPartitions", ORA_num_partition)
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .jdbc(ORA_connection_string, ORA_target_table, OraDBCredentials)
    }

    sparkSession.stop()
    val duration = (System.nanoTime() - startTime) / 1e9d
    println(s"Elapsed time for writing to Oracle (in seconds): $duration")
  }
  readGreenplumData()
}
