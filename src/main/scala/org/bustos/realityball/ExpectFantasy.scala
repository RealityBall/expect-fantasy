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
        if (((i + 1) - latestRegime) < 2) true
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
      v.foreach {
        case (pitcher, batters) =>
          {
            batters.foreach { batter =>
              {
                val baseFantasyScore = realityballData.latestFantasyData(game, batter)
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
                  if (game.id.contains(batter.team)) 1.0
                  else {
                    val visitorHomeBallparkAve = realityballData.ballparkBAbyDate(batter.team, game.date)
                    val homeHomeBallparkAve = realityballData.ballparkBAbyDate(game.homeTeam, game.date)
                    homeHomeBallparkAve.bAvg / visitorHomeBallparkAve.bAvg
                  }
                }
                val baTrend = 1.0 + realityballData.latestBAtrends(game, batter, pitcher) * 10.0
                val fanduelBase = baseFantasyScore.fanDuelMov
                val fanduelParkAdj = parkFantasyScoreAdj(batter, "fanduel")
                val draftKingsBase = baseFantasyScore.draftKingslMov
                val draftKingsParkAdj = parkFantasyScoreAdj(batter, "draftKings")
                val draftsterBase = baseFantasyScore.draftsterMov
                val draftsterParkAdj = parkFantasyScoreAdj(batter, "draftster")
                val prediction = FantasyPrediction(batter.id, game.id,
                  if ((fanduelBase.get * pitcherAdj * parkAdj * baTrend).isNaN) None else Some(fanduelBase.get * pitcherAdj * parkAdj * baTrend),
                  if ((draftKingsBase.get * pitcherAdj * parkAdj * baTrend).isNaN) None else Some(draftKingsBase.get * pitcherAdj * parkAdj * baTrend),
                  if ((draftsterBase.get * pitcherAdj * parkAdj * baTrend).isNaN) None else Some(draftsterBase.get * pitcherAdj * parkAdj * baTrend),
                  fanduelBase, draftKingsBase, draftsterBase,
                  if (pitcherAdj.isNaN) None else Some(pitcherAdj), Some(parkAdj), Some(baTrend))
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
