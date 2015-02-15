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

  val expectationDate = new DateTime(2014, 8, 24, 0, 0)

  def filteredByRegime(game: Game, lineup: List[Player]): List[Player] = {
    lineup.zipWithIndex.filter({
      case (p, i) => {
        val latestRegime = realityballData.latestLineupRegime(game, p)
        if (((i + 1) - latestRegime) <= 2) true
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

  val matchups = realityballData.games(expectationDate).foldLeft(Map.empty[Game, Map[Player, List[Player]]]) { (x, game) =>
    {
      val homeStarter = realityballData.playerFromRetrosheetId(game.startingHomePitcher, expectationDate.getYear.toString)
      val visitingStarter = realityballData.playerFromRetrosheetId(game.startingVisitingPitcher, expectationDate.getYear.toString)
      logger.info("Getting lineups for " + game.visitingTeam + " (" + visitingStarter.id + ") @ " + game.homeTeam + " (" + homeStarter.id + ")")

      val visitingLineup = filteredByInjury(filteredByRegime(game, realityballData.startingBatters(game, 0, expectationDate.getYear.toString)))
      val homeLineup = filteredByInjury(filteredByRegime(game, realityballData.startingBatters(game, 1, expectationDate.getYear.toString)))

      x + (game -> Map((homeStarter -> visitingLineup), (visitingStarter -> homeLineup)))
    }
  }

  def parkFantasyScoreAdj(batter: Player, fantasyGame: String): Option[Double] = {
    Some(0.0)
  }

  db.withSession { implicit session =>
    fantasyPredictionTable.ddl.drop
    fantasyPredictionTable.ddl.create
  }

  matchups.foreach({
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
                val pitcherAdj = {
                  if (pitcher.throwsWith == "R") {
                    (movingStats.RHbattingAverageMov.getOrElse(Double.NaN) / movingStats.battingAverageMov.getOrElse(Double.NaN) +
                      movingStats.RHonBasePercentageMov.getOrElse(Double.NaN) / movingStats.onBasePercentageMov.getOrElse(Double.NaN) +
                      movingStats.RHsluggingPercentageMov.getOrElse(Double.NaN) / movingStats.sluggingPercentageMov.getOrElse(Double.NaN)) / 3.0
                  } else {
                    (movingStats.LHbattingAverageMov.getOrElse(Double.NaN) / movingStats.battingAverageMov.getOrElse(Double.NaN) +
                      movingStats.LHonBasePercentageMov.getOrElse(Double.NaN) / movingStats.onBasePercentageMov.getOrElse(Double.NaN) +
                      movingStats.LHsluggingPercentageMov.getOrElse(Double.NaN) / movingStats.sluggingPercentageMov.getOrElse(Double.NaN)) / 3.0
                  }
                }
                val parkAdj = {
                  if (game.startingVisitingPitcher == pitcher.id) 1.0
                  else {
                    val visitorHomeBallparkAve = realityballData.ballparkBAbyDate(game.visitingTeam, game.date)
                    val homeHomeBallparkAve = realityballData.ballparkBAbyDate(game.homeTeam, game.date)
                    val lefty = ((homeHomeBallparkAve.ba.lhBAvg + homeHomeBallparkAve.slg.lhBAvg) / 2.0) /
                      ((visitorHomeBallparkAve.ba.lhBAvg + visitorHomeBallparkAve.slg.lhBAvg) / 2.0)
                    val righty = ((homeHomeBallparkAve.ba.rhBAvg + homeHomeBallparkAve.slg.rhBAvg) / 2.0) /
                      ((visitorHomeBallparkAve.ba.rhBAvg + visitorHomeBallparkAve.slg.rhBAvg) / 2.0)
                    if (batter.batsWith == "B") {
                      if (pitcher.throwsWith == "R") lefty
                      else righty
                    } else if (batter.batsWith == "R") righty
                    else lefty
                  }
                }
                val oddsAdj = {
                  val mlGap = (odds.homeML - odds.visitorML) / 2000.0
                  val signGap = if (mlGap > 0.0) 1.0 else -1.0
                  val factor = if (game.startingVisitingPitcher == pitcher.id) {
                    // Home batter
                    -1.0 * signGap
                  } else {
                    // Visiting Batter
                    1.0 * signGap
                  }
                  (1.0 + factor * mlGap.abs)
                }
                val baTrend = 1.0 + realityballData.latestBAtrends(game, batter, pitcher) * 10.0
                val fanduelBase = baseFantasyScore.fanDuel
                val fanduelVol = baseFantasyScoreVol.fanDuel
                val fanduelParkAdj = parkFantasyScoreAdj(batter, "fanduel")
                val draftKingsBase = baseFantasyScore.draftKings
                val draftKingsVol = baseFantasyScoreVol.draftKings
                val draftKingsParkAdj = parkFantasyScoreAdj(batter, "draftKings")
                val draftsterBase = baseFantasyScore.draftster
                val draftsterVol = baseFantasyScoreVol.draftster
                val draftsterParkAdj = parkFantasyScoreAdj(batter, "draftster")
                val prediction = FantasyPrediction(batter.id, game.id,
                  if ((fanduelBase.get * pitcherAdj * parkAdj * baTrend).isNaN) None else Some(fanduelBase.get * pitcherAdj * parkAdj * baTrend * oddsAdj),
                  if ((draftKingsBase.get * pitcherAdj * parkAdj * baTrend).isNaN) None else Some(draftKingsBase.get * pitcherAdj * parkAdj * baTrend * oddsAdj),
                  if ((draftsterBase.get * pitcherAdj * parkAdj * baTrend).isNaN) None else Some(draftsterBase.get * pitcherAdj * parkAdj * baTrend * oddsAdj),
                  fanduelBase, draftKingsBase, draftsterBase, fanduelVol, draftKingsVol, draftsterVol,
                  if (pitcherAdj.isNaN) None else Some(pitcherAdj), Some(parkAdj), Some(baTrend), Some(oddsAdj))
                db.withSession { implicit session =>
                  fantasyPredictionTable += prediction
                }
              }
            }
          }
      }
    }
  })

  logger.info("********************************")

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
