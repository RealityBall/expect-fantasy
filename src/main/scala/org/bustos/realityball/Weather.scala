package org.bustos.realityball

import spray.can.Http
import spray.http._
import spray.httpx.ResponseTransformation._
import scala.concurrent._
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.{ Timeout, ByteString }
import akka.actor.{ ActorSystem }
import akka.io.IO
import spray.json._
import DefaultJsonProtocol._
import org.slf4j.LoggerFactory
import RealityballConfig._

object WeatherLoadRecords {
  case class ForecastDate(pretty: String)
  case class ForecastPeriod(conditions: String, pop: Int, date: ForecastDate)
  case class SimpleForecast(forecastday: Array[ForecastPeriod])
  case class Txt_Forecast(date: String)
  case class Response(version: String, termsofService: String)
  case class Forecast(txt_forecast: Txt_Forecast, simpleforecast: SimpleForecast)

  case class FctTime(pretty: String, epoch: String)
  case class EnglishMetric(english: String, metric: String)
  case class HourlyForecastDetail(FCTTIME: FctTime, pop: String, condition: String, temp: EnglishMetric, wspd: EnglishMetric)

  case class ObservationDate(pretty: String, year: String, mon: String, mday: String, hour: String, min: String, tzname: String)
  case class Observation(date: ObservationDate, tempm: String, tempi: String, wspdm: String, wspdi: String, wdird: String, wdire: String, precipm: String, precipi: String, conds: String)
  case class HistoricalObservation(date: ObservationDate, observations: Array[Observation])

  case class HourlyForecast(response: Response, hourly_forecast: Array[HourlyForecastDetail])
  case class DailyForecast(response: Response, forecast: Forecast)
  case class HistoricalObservations(response: Response, history: HistoricalObservation)
}

object WeatherJsonProtocol extends DefaultJsonProtocol {
  import WeatherLoadRecords._

  implicit val fcTimeFormat = jsonFormat2(FctTime)
  implicit val emFormat = jsonFormat2(EnglishMetric)
  implicit val hourlyForecastDetailFormat = jsonFormat5(HourlyForecastDetail)
  implicit val forecastDateFormat = jsonFormat1(ForecastDate)
  implicit val forecastPeriodFormat = jsonFormat3(ForecastPeriod)
  implicit val simpleForecastFormat = jsonFormat1(SimpleForecast)
  implicit val txtForecastFormat = jsonFormat1(Txt_Forecast)
  implicit val responseFormat = jsonFormat2(Response)
  implicit val forecastFormat = jsonFormat2(Forecast)
  implicit val observationDateFormat = jsonFormat7(ObservationDate)
  implicit val observationFormat = jsonFormat10(Observation)
  implicit val historicalObservationForamt = jsonFormat2(HistoricalObservation)

  implicit val hourlyForecastFormat = jsonFormat2(HourlyForecast)
  implicit val dailyForecastFormat = jsonFormat2(DailyForecast)
  implicit val historicalObservationsFormat = jsonFormat2(HistoricalObservations)
}

class Weather(postalCode: String) {

  import WeatherJsonProtocol._
  import WeatherLoadRecords._
  import RealityballRecords._

  val logger = LoggerFactory.getLogger(getClass)

  implicit val system = ActorSystem()
  implicit val timeout: Timeout = Timeout(5 minutes)

  val timeOfRequest: Long = System.currentTimeMillis / 1000

  val hourlyForecasts: List[HourlyForecastDetail] = {
    val request = HttpRequest(HttpMethods.GET, WUNDERGROUND_APIURL + WUNDERGROUND_APIKEY + "/hourly/q/" + postalCode + ".json")
    val response = Await.result((IO(Http) ? request).mapTo[HttpResponse], 30 seconds) ~> unmarshal[String]
    response.parseJson.convertTo[HourlyForecast].hourly_forecast.toList
  }

  def forecastConditions(time: String): GameConditions = {
    val format = new java.text.SimpleDateFormat("yyyyMMdd HH:mm")
    val date = format.parse(time)
    hourlyForecasts.reverse.find { _.FCTTIME.epoch.toLong < date.getTime } match {
      case Some(x) => GameConditions("", "", "", false, x.temp.english.toInt, "", x.wspd.english.toInt, "", x.pop, x.condition)
      case None    => null
    }
  }

  def historicalConditions(time: String): GameConditions = {
    val request = HttpRequest(HttpMethods.GET, WUNDERGROUND_APIURL + WUNDERGROUND_APIKEY + "/history_" + time.split(' ')(0) + "/q/" + postalCode + ".json")
    val response = Await.result((IO(Http) ? request).mapTo[HttpResponse], 30 seconds) ~> unmarshal[String]
    response.parseJson.convertTo[HistoricalObservations].history.observations.toList.reverse.find { x => x.date.year + x.date.mon + x.date.mday + " " + x.date.hour + ":" + x.date.min < time } match {
      case Some(x) => GameConditions("", "", "", false, x.tempi.toDouble.toInt, "", x.wspdi.toDouble.toInt, "", "", x.conds)
      case None    => null
    }
  }

}
