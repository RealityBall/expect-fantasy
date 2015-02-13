package org.bustos.realityball

import org.joda.time._
import org.joda.time.format._
import scala.slick.driver.MySQLDriver.simple._
import scala.util.Properties.envOrElse
import scala.math.{ pow, sqrt }

object RealityballConfig {

  val GamedayURL = "http://mlb.com/"
  val MlbURL = "http://mlb.mlb.com/"
  val DataRoot = "/Users/mauricio/Google Drive/Projects/fantasySports/data/"

  val WUNDERGROUND_APIURL = "http://api.wunderground.com/api/"
  val WUNDERGROUND_APIKEY = "eeb51f60b8bd49aa"

  val db = {
    val mysqlURL = envOrElse("MLB_MYSQL_URL", "jdbc:mysql://localhost:3306/mlbretrosheet")
    val mysqlUser = envOrElse("MLB_MYSQL_USER", "root")
    val mysqlPassword = envOrElse("MLB_MYSQL_PASSWORD", "")
    Database.forURL(mysqlURL, driver = "com.mysql.jdbc.Driver", user = mysqlUser, password = mysqlPassword)
  }

  val CcyymmddFormatter = DateTimeFormat.forPattern("yyyyMMdd")
  val CcyymmddDelimFormatter = DateTimeFormat.forPattern("yyyy_MM_dd")
  val CcyymmddSlashDelimFormatter = DateTimeFormat.forPattern("yyyy/MM/dd")
  val YearFormatter = DateTimeFormat.forPattern("yyyy")

  val MovingAverageWindow = 25
  val TeamMovingAverageWindow = 10

  val MovingAverageExponentialWeights: List[Double] = {
    val alpha = 0.9

    val rawWeights = (1 to MovingAverageWindow).map({ x => pow(alpha, x.toDouble) })
    val totalWeight = rawWeights.foldLeft(0.0)(_ + _)
    rawWeights.map(_ / totalWeight).toList
  }

}
