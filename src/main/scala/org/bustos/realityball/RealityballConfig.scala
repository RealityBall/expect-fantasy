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

  val StrikeOutPitcherStyleThreshold = (0.301 + 0.076 / 2.0)
  val FlyballPitcherStyleThreshold = (0.339 + 0.073 / 2.0)
  val GroundballPitcherStyleThreshold = (0.353 + 0.088 / 2.0)

  // Matchups (Pitcher/Batter)
  val MatchupBase = 0.5142
  val MatchupNeutralNeutral = 0.5024
  val MatchupStrikeOutNeutral = 0.4439
  val MatchupFlyBallNeutral = 0.5678
  val MatchupGroundballNeutral = 0.5519
  val MatchupNeutralStrikeOut = 0.4587
  val MatchupStrikeOutStrikeOut = 0.3812
  val MatchupFlyballStrikeOut = 0.5257
  val MatchupGroundballStrikeOut = 0.4928
  val MatchupNeutralFlyball = 0.5325
  val MatchupStrikeOutFlyball = 0.4595
  val MatchupFlyballFlyball = 0.6132
  val MatchupGroundballFlyball = 0.5697
  val MatchupNeutralGroundball = 0.5065
  val MatchupStrikeOutGroundball = 0.4071
  val MatchupFlyballGroundball = 0.5524
  val MatchupGroundballGroundball = 0.5144

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
