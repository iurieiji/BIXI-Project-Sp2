package bixi

import java.io.PrintWriter

import org.apache.hadoop.fs.Path

import io.circe.generic.auto._

object Main extends App with HDFS {

  if(fs.delete(new Path(s"$uri/user/winter2020/iuri/bixi/external"),true))
    println("Folder Bixi deleted before Instantiate!")

  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/bixi/external"))

  val listSystemInformation: Either[Throwable, SystemInformation] =
    "https://gbfs.velobixi.com/gbfs/en/system_information.json".pipe(ReadJsonFrom.urlInto[SystemInformation])

  val listStationInformation: Either[Throwable, StationInformation] =
    "station_information.json".pipe(ReadJsonFrom.resourceInto[StationInformation])

  val systemHdfsPath = fs.create(new Path(
    s"$uri/user/winter2020/iuri/bixi/external/system_information/system_information.csv"))
  val systemSchema: String = "last_updated,ttl,system_id,language,name,short_name,operator,url,purchase_url," +
    "start_date,phone_number,email,license_url,timezone"
  val writerSystem = new PrintWriter(systemHdfsPath)
  writerSystem.write(systemSchema)
  val systemCsv = listSystemInformation.map(write => SystemInformation.toCsv(write))
  for (line <- systemCsv) {
    writerSystem.write(System.lineSeparator)
    writerSystem.write(line)
  }
  writerSystem.close()

  val stationHdfsPath = fs.create(new Path(
    s"$uri/user/winter2020/iuri/bixi/external/station_information/station_information.csv"))
  val stationSchema: String = "station_id,external_id,name,short_name,lat,lon,rental_methods_key," +
    "rental_methods_value,capacity,electric_bike_surcharge_waiver,is_charging,eightd_has_key_dispenser,has_kiosk"
  val writerStation = new PrintWriter(stationHdfsPath)
  writerStation.write(stationSchema)
  val listStationLength = listStationInformation.map(_.data.stations.size).toOption.get
  var listStationIndex = 0
  while (listStationIndex < listStationLength) {
    val stationCsv = listStationInformation.map(write => StationInformation.toCsv(write, listStationIndex))
    for (line <- stationCsv) {
      writerStation.write(System.lineSeparator)
      writerStation.write(line)
    }
    listStationIndex = listStationIndex + 1
  }
  writerStation.close()

  stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict")
  stmt.executeUpdate("""CREATE DATABASE IF NOT EXISTS winter2020_iuri""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_system_information""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_station_information""".stripMargin)
  stmt.executeUpdate("""DROP TABLE enriched_station_information""".stripMargin)

  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE ext_system_information (
      |last_updated  INT,
      |ttl           INT,
      |system_id     STRING,
      |language      STRING,
      |name          STRING,
      |short_name    STRING,
      |operator      STRING,
      |url           STRING,
      |purchase_url  STRING,
      |start_date    STRING,
      |phone_number  STRING,
      |email         STRING,
      |license_url   STRING,
      |timezone      STRING)
      |row format DELIMITED
      |fields TERMINATED BY ','
      |stored as textfile
      |LOCATION '/user/winter2020/iuri/bixi/external/system_information'
      |tblproperties ("skip.header.line.count"="1")
      |""".stripMargin
  )

  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE ext_station_information (
      |station_id               INT,
      |external_id              STRING,
      |name                     STRING,
      |short_name               STRING,
      |Lat                      DOUBLE,
      |Lon                      DOUBLE,
      |rental_methods_key       STRING,
      |rental_methods_value     STRING,
      |Capacity                 INT,
      |e_b_s_w                  BOOLEAN,
      |is_charging              BOOLEAN,
      |eightd_has_key_dispenser BOOLEAN,
      |has_kiosk                BOOLEAN
      |)
      |row format DELIMITED
      |fields TERMINATED BY ','
      |stored as textfile
      |LOCATION '/user/winter2020/iuri/bixi/external/station_information'
      |tblproperties ("skip.header.line.count"="1")
      |""".stripMargin
  )

  stmt.executeUpdate(
    """CREATE TABLE winter2020_iuri.enriched_station_information (
      |system_id     STRING,
      |timezone      STRING,
      |station_id    INT,
      |name          STRING,
      |short_name    STRING,
      |lat           DOUBLE,
      |lon           DOUBLE,
      |capacity      INT
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS PARQUET""".stripMargin
  )

  stmt.executeUpdate(
    """INSERT OVERWRITE TABLE winter2020_iuri.enriched_station_information
      |SELECT
      |sy.system_id,
      |sy.timezone,
      |st.station_id,
      |st.name,
      |st.short_name,
      |st.Lat,
      |st.Lon,
      |st.Capacity
      |FROM ext_station_information st
      |CROSS JOIN ext_system_information sy
      """.stripMargin
  )

}

