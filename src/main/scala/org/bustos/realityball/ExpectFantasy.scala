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

  var ballparkData = Map.empty[(String, String), BattingAverageSummaries]

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

  def processMatchups(date: DateTime) = {
    matchups(date).foreach({
      case (game, v) => {
        val odds = realityballData.odds(game)
        v.foreach {
          case (pitcher, batters) =>
            {
              batters.foreach { batter =>
                {
                  val baseFantasyScore = realityballData.latestFantasyData(game, batter)
                  val baseFantasyScoreVol = realityballData.latestFantasyVolData(game, batter)
                  val movingStats = realityballData.latestBAdata(game, batter)
                  if (batter.id == "cabre001") {
                    //                    println("")
                  }
                  val pitcherAdj = {
                    val rhFs = realityballData.fsPerPa(batter, game.date, "R")
                    val lhFs = realityballData.fsPerPa(batter, game.date, "L")
                    val ratio = if (pitcher.throwsWith == "R") {
                      if (lhFs != 0.0) (rhFs / lhFs - 1.0) else 0.0
                    } else {
                      if (rhFs != 0.0) (lhFs / rhFs - 1.0) else 0.0
                    }
                    ratio.min(10.0).max(-10.0)
                  }
                  val parkAdj = {
                    if (game.startingVisitingPitcher == pitcher.id) 0.0
                    else {
                      val visitorHomeBallparkAve = {
                        if (!ballparkData.contains((game.visitingTeam, game.date))) {
                          ballparkData += ((game.visitingTeam, game.date) -> realityballData.ballparkBAbyDate(game.visitingTeam, game.date))
                        }
                        ballparkData((game.visitingTeam, game.date))
                      }
                      val homeHomeBallparkAve = {
                        if (!ballparkData.contains((game.homeTeam, game.date))) {
                          ballparkData += ((game.homeTeam, game.date) -> realityballData.ballparkBAbyDate(game.homeTeam, game.date))
                        }
                        ballparkData((game.homeTeam, game.date))
                      }
                      ((homeHomeBallparkAve.ba.bAvg + homeHomeBallparkAve.slg.bAvg) / 2.0) /
                        ((visitorHomeBallparkAve.ba.bAvg + visitorHomeBallparkAve.slg.bAvg) / 2.0) - 1.0
                    }
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
                  val fanduelBase = baseFantasyScore.fanDuel
                  val fanduelVol = baseFantasyScoreVol.fanDuel
                  val fanduelParkAdj = parkFantasyScoreAdj(batter, "fanduel")
                  val draftKingsBase = baseFantasyScore.draftKings
                  val draftKingsVol = baseFantasyScoreVol.draftKings
                  val draftKingsParkAdj = parkFantasyScoreAdj(batter, "draftKings")
                  val draftsterBase = baseFantasyScore.draftster
                  val draftsterVol = baseFantasyScoreVol.draftster
                  val draftsterParkAdj = parkFantasyScoreAdj(batter, "draftster")

                  val valuationFanDuel = Intercept +
                    BetaFanDuelBase * fanduelBase.getOrElse(Double.NaN) +
                    BetaPitcherAdj * pitcherAdj +
                    BetaParkAdj * parkAdj +
                    BetaBaTrendAdj * baTrend +
                    BetaOddsAdj * oddsAdj +
                    BetaMatchupAdj * matchupAdj
                  val prediction = FantasyPrediction(batter.id, game.id, None,
                    if (valuationFanDuel.isNaN) None else Some(valuationFanDuel),
                    if ((draftKingsBase.getOrElse(Double.NaN) * pitcherAdj * parkAdj * baTrend * matchupAdj).isNaN) None else Some(draftKingsBase.get * pitcherAdj * parkAdj * baTrend * oddsAdj * matchupAdj),
                    if ((draftsterBase.getOrElse(Double.NaN) * pitcherAdj * parkAdj * baTrend * matchupAdj).isNaN) None else Some(draftsterBase.get * pitcherAdj * parkAdj * baTrend * oddsAdj * matchupAdj),
                    fanduelBase, draftKingsBase, draftsterBase, fanduelVol, draftKingsVol, draftsterVol,
                    if (pitcherAdj.isNaN) None else Some(pitcherAdj), Some(parkAdj), Some(baTrend), Some(oddsAdj), Some(matchupAdj))
                  db.withSession { implicit session =>
                    fantasyPredictionTable += prediction
                  }
                }
              }
            }
        }
      }
    })
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
    processMatchups(expectationDate.plusDays(x))
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
