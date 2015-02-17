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

    val rawWeights = (0 to MovingAverageWindow - 1).map({ x => pow(alpha, x.toDouble) })
    val totalWeight = rawWeights.foldLeft(0.0)(_ + _)
    rawWeights.map(_ / totalWeight).toList
  }

  val StrikeOut = "StrikeOut"
  val FlyBall = "FlyBall"
  val GroundBall = "GroundBall"
  val BaseOnBalls = "BaseOnBalls"

  val StrikeOutBatterStyleThreshold = (0.206 + 0.062)
  val FlyballBatterStyleThreshold = (0.381 + 0.057)
  val GroundballBatterStyleThreshold = (0.328 + 0.070)
  val BaseOnBallsBatterStyleThreshold = (0.085 + 0.031)

  val StrikeOutPitcherStyleThreshold = (0.301 + 0.076 / 2.0)
  val FlyballPitcherStyleThreshold = (0.339 + 0.073 / 2.0)
  val GroundballPitcherStyleThreshold = (0.353 + 0.088 / 2.0)

  val MatchupNeutral = 0.592
  val StrikeOutStrikeOut = 0.381
  val FlyballFlyball = 0.749
  val GroundballFlyball = 0.707
  val GroundballStrikeOut = 0.496
  val StrikeOutGroundball = 0.467

  // Valuation Model
  /*
  val Intercept = -0.9234
  val BetaFanDuelBase = 0.4986
  val BetaOddsAdj = 0.9994
  val BetaMatchupAdj = 1.3730
  *
  */
  val Intercept = -1.2611
  val BetaFanDuelBase = 0.4866
  val BetaPitcherAdj = 0.3135
  val BetaParkAdj = 0.0257
  val BetaBaTrendAdj = 0.0520
  val BetaOddsAdj = 0.99475
  val BetaMatchupAdj = 1.3640

}
