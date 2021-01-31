package bixi

case class SystemInformation(last_updated: Int, ttl: Int, data: DataInfo)
case class DataInfo(
                  system_id: String,
                  language: String,
                  name: String,
                  short_name: String,
                  operator: String,
                  url: String,
                  purchase_url: String,
                  start_date: String,
                  phone_number: String,
                  email: String,
                  license_url: String,
                  timezone: String)

object SystemInformation {
  def apply(csvLine: String): SystemInformation = {
    val t: Array[String] = csvLine.split(",", -1)
    new SystemInformation(t(0).toInt, t(1).toInt, DataInfo(t(2), t(3), t(4), t(5), t(6), t(7), t(8), t(9), t(10),
      t(11), t(12), t(13)))
  }

  def toCsv(systemInformation: SystemInformation): String = {
    systemInformation.last_updated + "," +
    systemInformation.ttl + "," +
    systemInformation.data.system_id + "," +
    systemInformation.data.language + "," +
    systemInformation.data.name + "," +
    systemInformation.data.short_name + "," +
    systemInformation.data.operator + "," +
    systemInformation.data.url + "," +
    systemInformation.data.purchase_url + "," +
    systemInformation.data.start_date + "," +
    systemInformation.data.phone_number + "," +
    systemInformation.data.email + "," +
    systemInformation.data.license_url + "," +
    systemInformation.data.timezone
  }
}
