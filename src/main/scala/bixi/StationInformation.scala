package bixi

case class StationInformation(last_updated: Int, ttl: Int, data: Data)
case class Data(stations: List[Stations])
case class Stations(
                   station_id: String, external_id: String, name: String, short_name: String,
                   lat: Float, lon: Float, rental_methods: List[String],
                   capacity: Int,
                   electric_bike_surcharge_waiver: Boolean,
                   is_charging: Boolean,
                   eightd_has_key_dispenser: Boolean,
                   has_kiosk: Boolean)

object StationInformation {
  def apply(csvLine: String): StationInformation = {
    val r: Array[String] = csvLine.split(",", -1)
    new StationInformation(r(0).toInt, r(1).toInt, Data(List(Stations(r(2), r(3), r(4),
      r(5), r(6).toFloat, r(7).toFloat, List(r(8), r(9)), r(10).toInt, r(11).toBoolean,
      r(12).toBoolean, r(13).toBoolean, r(14).toBoolean))))
  }

  def toCsv(stationInformation: StationInformation, stationPos: Int): String = {
    stationInformation.data.stations(stationPos).station_id + "," +
    stationInformation.data.stations(stationPos).external_id + "," +
    stationInformation.data.stations(stationPos).name + "," +
    stationInformation.data.stations(stationPos).short_name + "," +
    stationInformation.data.stations(stationPos).lat + "," +
    stationInformation.data.stations(stationPos).lon + "," +
    stationInformation.data.stations(stationPos).rental_methods(0) + "," +
    stationInformation.data.stations(stationPos).rental_methods(1) + "," +
    stationInformation.data.stations(stationPos).capacity + "," +
    stationInformation.data.stations(stationPos).electric_bike_surcharge_waiver + "," +
    stationInformation.data.stations(stationPos).is_charging + "," +
    stationInformation.data.stations(stationPos).eightd_has_key_dispenser + "," +
    stationInformation.data.stations(stationPos).has_kiosk
  }
}
