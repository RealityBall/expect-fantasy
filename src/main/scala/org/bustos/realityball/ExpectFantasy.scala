package org.bustos.realityball

import org.joda.time._
import org.joda.time.format._
import org.slf4j.LoggerFactory
import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable
import RealityballRecords._
import RealityballConfig._

object ExpectFantasy extends App {

  val realityballData = new RealityballData
  val logger = LoggerFactory.getLogger(getClass)

  var ballparkData = Map.empty[(String, String), Double]

  def matchups(date: DateTime) = {

    def filteredByRegime(game: Game, lineup: List[Player]): List[Player] = {
      lineup.zipWithIndex.filter({
        case (p, i) => {
          val latestRegime = realityballData.latestLineupRegime(game, p)
          if (p.position == "P") {
            logger.info("\t" + p.firstName + " " + p.lastName + " (" + p.id + ") filtered because of position (" + p.position + ")")
            false
          } else if ((((i + 1) - latestRegime) <= 2 && latestRegime > 0) || i < 5) true
          else {
            logger.info("\t" + p.firstName + " " + p.lastName + " (" + p.id + ") filtered because of lineup regime (" + latestRegime + " -> " + (i + 1) + ")")
            false
          }
        }
      }).unzip._1
    }

    def filteredByInjury(lineup: List[Player]): List[Player] = {
      lineup
    }

    def filteredByWeather(lineup: List[Player]): List[Player] = {
      lineup
    }

    realityballData.games(date).foldLeft(Map.empty[Game, Map[Player, List[Player]]]) { (x, game) =>
      {
        val homeStarter = realityballData.playerFromRetrosheetId(game.startingHomePitcher, date.getYear.toString)
        val visitingStarter = realityballData.playerFromRetrosheetId(game.startingVisitingPitcher, date.getYear.toString)
        logger.info("Getting lineups for " + game.visitingTeam + " (" + visitingStarter.id + ") @ " + game.homeTeam + " (" + homeStarter.id + ")")

        val visitingLineup = filteredByInjury(filteredByRegime(game, realityballData.startingBatters(game, 0, date.getYear.toString)))
        val homeLineup = filteredByInjury(filteredByRegime(game, realityballData.startingBatters(game, 1, date.getYear.toString)))

        x + (game -> Map((homeStarter -> visitingLineup), (visitingStarter -> homeLineup)))
      }
    }
  }

  def parkFantasyScoreAdj(batter: Player, fantasyGame: String): Option[Double] = {
    Some(0.0)
  }

  def bayesianPrediction(odds: Double, pitcher: Double, matchup: Double, rate: Double, park: Double, overUnder: Double): Double = {
    var start = 1.0
    0.0
  }

  def processedBatters(game: Game, pitcher: Player, batters: List[Player], odds: GameOdds): List[FantasyPrediction] = {
    batters.map { batter =>
      val recentFantasyData = realityballData.recentFantasyData(game, batter, 1)
      val baseFantasyScoreMov = realityballData.latestFantasyMovData(game, batter)
      val baseFantasyScoreVol = realityballData.latestFantasyVolData(game, batter)
      val movingStats = realityballData.latestBAdata(game, batter)
      val pitcherAdj = {
        val rhFs = realityballData.fsPerPa(batter, game.date, "R")
        val lhFs = realityballData.fsPerPa(batter, game.date, "L")

        //multi break point bayes calculations
        //per player model
        //trend adj numbers(peter)
        //salary model (peter)
        //over under

        val average = (rhFs + lhFs) / 2.0

        val ratio = if (pitcher.throwsWith == "R") {
          (rhFs - lhFs) / average
        } else {
          (lhFs - rhFs) / average
        }
        ratio.min(10.0).max(-10.0)
      }
      val parkAdj = {
        if (!ballparkData.contains((game.homeTeam, game.date))) {
          ballparkData += ((game.homeTeam, game.date) -> realityballData.ballparkFantasy(game.homeTeam, game.date))
        }
        ballparkData((game.homeTeam, game.date))
      }
      val overUnderML = {
        if (odds.overUnderML < 0.0) odds.overUnderML + 100.0
        else odds.overUnderML - 100.0
      }
      val oddsAdj = {
        val homeOdds = {
          if (odds.homeML > 0) (odds.homeML - 100.0) / 100.0
          else (odds.homeML + 100.0) / odds.homeML
        }
        val visitorOdds = {
          if (odds.visitorML > 0) (odds.visitorML - 100.0) / 100.0
          else (odds.visitorML + 100.0) / odds.visitorML
        }
        val mlGap = homeOdds - visitorOdds
        val signGap = if (mlGap > 0.0) 1.0 else -1.0
        val factor = if (game.startingVisitingPitcher == pitcher.id) {
          // Home batter
          -1.0 * signGap
        } else {
          // Visiting Batter
          1.0 * signGap
        }
        factor * mlGap.abs
      }
      val baTrend = realityballData.latestBAtrends(game, batter, pitcher)
      val matchupAdj = {
        val batterStyle = realityballData.batterStyle(batter, game, pitcher)
        val pitcherStyle = realityballData.pitcherStyle(pitcher, game)
        if (pitcherStyle == StrikeOut) {
          if (batterStyle == StrikeOut) {
            (MatchupStrikeOutStrikeOut - MatchupBase) / MatchupBase
          } else if (batterStyle == FlyBall) {
            (MatchupStrikeOutFlyball - MatchupBase) / MatchupBase
          } else if (batterStyle == GroundBall) {
            (MatchupStrikeOutGroundball - MatchupBase) / MatchupBase
          } else {
            (MatchupStrikeOutNeutral - MatchupBase) / MatchupBase
          }
        } else if (pitcherStyle == FlyBall) {
          if (batterStyle == StrikeOut) {
            (MatchupFlyballStrikeOut - MatchupBase) / MatchupBase
          } else if (batterStyle == FlyBall) {
            (MatchupFlyballFlyball - MatchupBase) / MatchupBase
          } else if (batterStyle == GroundBall) {
            (MatchupFlyballGroundball - MatchupBase) / MatchupBase
          } else {
            (MatchupFlyBallNeutral - MatchupBase) / MatchupBase
          }
        } else if (pitcherStyle == GroundBall) {
          if (batterStyle == StrikeOut) {
            (MatchupGroundballStrikeOut - MatchupBase) / MatchupBase
          } else if (batterStyle == FlyBall) {
            (MatchupGroundballFlyball - MatchupBase) / MatchupBase
          } else if (batterStyle == GroundBall) {
            (MatchupGroundballGroundball - MatchupBase) / MatchupBase
          } else {
            (MatchupGroundballNeutral - MatchupBase) / MatchupBase
          }
        } else {
          if (batterStyle == StrikeOut) {
            (MatchupNeutralStrikeOut - MatchupBase) / MatchupBase
          } else if (batterStyle == FlyBall) {
            (MatchupNeutralFlyball - MatchupBase) / MatchupBase
          } else if (batterStyle == GroundBall) {
            (MatchupNeutralGroundball - MatchupBase) / MatchupBase
          } else {
            (MatchupNeutralNeutral - MatchupBase) / MatchupBase
          }
        }
      }
      val fanduelBase = baseFantasyScoreMov.fanDuel
      val fanduelVol = baseFantasyScoreVol.fanDuel
      val fanduelParkAdj = parkFantasyScoreAdj(batter, "fanduel")
      val draftKingsBase = baseFantasyScoreMov.draftKings
      val draftKingsVol = baseFantasyScoreVol.draftKings
      val draftKingsParkAdj = parkFantasyScoreAdj(batter, "draftKings")
      val draftsterBase = baseFantasyScoreMov.draftster
      val draftsterVol = baseFantasyScoreVol.draftster
      val draftsterParkAdj = parkFantasyScoreAdj(batter, "draftster")

      FantasyPrediction(batter.id, game.id, batter.position,
        bayesianPrediction(oddsAdj, pitcherAdj, matchupAdj, recentFantasyData.head.productionRate.get, parkAdj, odds.overUnder), recentFantasyData.head.productionRate,
        recentFantasyData.head.fanDuel, recentFantasyData.head.draftKings, recentFantasyData.head.draftster,
        fanduelBase, draftKingsBase, draftsterBase, fanduelVol, draftKingsVol, draftsterVol,
        if (pitcherAdj.isNaN) None else Some(pitcherAdj), Some(parkAdj), Some(baTrend),
        Some(oddsAdj), Some(odds.overUnder), Some(overUnderML), Some(matchupAdj))
    }
  }

  def processedGames(game: Game, pitcherVbatters: Map[Player, List[Player]], odds: GameOdds): List[FantasyPrediction] = {
    pitcherVbatters.flatMap {
      case (pitcher, batters) => processedBatters(game, pitcher, batters, odds)
    }.toList
  }

  def processedMatchups(date: DateTime): List[FantasyPrediction] = {
    matchups(date).flatMap {
      case (game, pitcherVbatters) => processedGames(game, pitcherVbatters, realityballData.odds(game))
    }.toList
  }

  val expectationDate = new DateTime(2014, 3, 30, 0, 0)
  //val expectationDate = new DateTime(2014, 6, 10, 0, 0)

  db.withSession { implicit session =>
    fantasyPredictionTable.ddl.drop
    fantasyPredictionTable.ddl.create
  }

  (0 to 190).foreach { x =>
    logger.info("*****************************")
    logger.info("*** Creating predictions for " + CcyymmddFormatter.print(expectationDate.plusDays(x)))
    logger.info("*****************************")
    val matchups = processedMatchups(expectationDate.plusDays(x))
    val byPosition = matchups.groupBy(_.position).map({ case (k, v) => (k, v.sortBy({ _.fanduelActual }).reverse)})
    byPosition.foreach( { case (k, v) =>
        logger.info("Position: " + k + " " + v.map({_.id}).mkString(","))
    })
    db.withSession { implicit session =>
      fantasyPredictionTable ++= matchups
    }

  }

  // Universe Construction:
  //   Find batters expected to be in lineup tomorrow
  //   Remove those that are:
  //    - out of their lineup regime
  //    - at risk of a weather delay
  //    - out due to injury
  //   Matchup each batter with the game setup
  //    - Opposing pitcher
  //    - Venue
  //
  // Compute expected fantasy score
  //    - Base expectation
  //    - Adjustments
  //        - Style
  //        - Venue
  //        - Relative strength

}
