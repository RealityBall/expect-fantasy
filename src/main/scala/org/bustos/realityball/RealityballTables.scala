package org.bustos.realityball

import org.joda.time._
import scala.slick.driver.MySQLDriver.simple._
import scala.slick.lifted.{ ProvenShape, ForeignKeyQuery }
import spray.json._
import DefaultJsonProtocol._

object RealityballRecords {
  import scala.collection.mutable.Queue

  case class Statistic(val date: String, var total: Double, var rh: Double, var lh: Double)
  case class StatisticInputs(var totalNumer: Double, var totalDenom: Double, var rhNumer: Double, var rhDenom: Double, var lhNumer: Double, var lhDenom: Double)
  case class RunningHitterData(lineupPosition: Queue[Int], ba: Queue[StatisticInputs], obp: Queue[StatisticInputs], slugging: Queue[StatisticInputs], fantasy: Map[String, Queue[Statistic]],
                               strikeOuts: Queue[Statistic], flyBalls: Queue[Statistic], groundBalls: Queue[Statistic], baseOnBalls: Queue[Statistic])
  case class Team(year: String, mnemonic: String, league: String, city: String, name: String, site: String, zipCode: String,
                  mlbComId: String, mlbComName: String, timeZone: String, coversComId: String, coversComName: String)

  case class BattingAverageObservation(date: String, bAvg: Double, lhBAvg: Double, rhBAvg: Double)
  case class BattingAverageSummaries(ba: BattingAverageObservation, obp: BattingAverageObservation, slg: BattingAverageObservation)

  case class Player(id: String, year: String, lastName: String, firstName: String, batsWith: String, throwsWith: String, team: String, position: String)
  case class PlayerSummary(id: String, lineupRegime: Int, RHatBats: Int, LHatBats: Int, games: Int, mlbId: String, brefId: String, espnId: String)
  case class PitcherSummary(id: String, daysSinceLastApp: Int, wins: Int, losses: Int, saves: Int, games: Int, mlbId: String, brefId: String, espnId: String)
  case class PlayerData(meta: Player, appearances: PlayerSummary)
  case class PitcherData(meta: Player, appearances: PitcherSummary)

  case class PitcherDaily(id: String, game: String, date: String, var daysSinceLastApp: Int, opposing: String, var win: Int, var loss: Int, var save: Int,
                          var hits: Int, var walks: Int, var hitByPitch: Int, var strikeOuts: Int, var groundOuts: Int, var flyOuts: Int,
                          var earnedRuns: Int, var outs: Int, var shutout: Boolean, var noHitter: Boolean, var pitches: Int, var balls: Int, var style: String)

  case class Game(id: String, var homeTeam: String, var visitingTeam: String, var site: String, var date: String, var number: Int, var startingHomePitcher: String, var startingVisitingPitcher: String)
  case class GameConditions(id: String, var startTime: String, var daynight: String, var usedh: Boolean,
                            var temp: Int, var winddir: String, var windspeed: Int, var fieldcond: String, var precip: String, var sky: String)
  case class GamedaySchedule(var id: String, homeTeam: String, visitingTeam: String, site: String, date: String, number: Int,
                             result: String, winningPitcher: String, losingPitcher: String, record: String, var startingHomePitcher: String, var startingVisitingPitcher: String,
                             startTime: String, temp: Int, winddir: String, windspeed: Int, precip: String, sky: String)
  case class GameScoring(id: String, var umphome: String, var ump1b: String, var ump2b: String, var ump3b: String, var howscored: String,
                         var timeofgame: Int, var attendance: Int, var wp: String, var lp: String, var save: String)
  case class GameOdds(var id: String, var visitorML: Int, var homeML: Int, var overUnder: Double, var overUnderML: Int)
  case class FullGameInfo(schedule: GamedaySchedule, odds: GameOdds)
  case class InjuryReport(mlbId: String, reportTime: String, injuryReportDate: String, status: String, dueBack: String, injury: String)

  case class FantasyPrediction(id: String, gameId: String, productionRate: Option[Double],
                               eFanduel: Option[Double], eDraftKings: Option[Double], eDraftster: Option[Double],
                               fanduelBase: Option[Double], draftKingsBase: Option[Double], draftsterBase: Option[Double],
                               fanduelVol: Option[Double], draftKingsVol: Option[Double], draftsterVol: Option[Double],
                               pitcherAdj: Option[Double], parkAdj: Option[Double], baTrendAdj: Option[Double], oddsAdj: Option[Double], matchupAdj: Option[Double])
  case class HitterStatsMoving(date: String, id: String, pitcherId: String, pitcherIndex: Int,
                               RHbattingAverageMov: Option[Double], LHbattingAverageMov: Option[Double], battingAverageMov: Option[Double],
                               RHonBasePercentageMov: Option[Double], LHonBasePercentageMov: Option[Double], onBasePercentageMov: Option[Double],
                               RHsluggingPercentageMov: Option[Double], LHsluggingPercentageMov: Option[Double], sluggingPercentageMov: Option[Double],
                               RHstyle: String, LHstyle: String, style: String)
  case class HitterFantasyDaily(date: String, id: String, gameId: String, side: Int, pitcherId: String, pitcherIndex: Int, productionRate: Option[Double],
                                RHfanDuel: Option[Double], LHfanDuel: Option[Double], fanDuel: Option[Double],
                                RHdraftKings: Option[Double], LHdraftKings: Option[Double], draftKings: Option[Double],
                                RHdraftster: Option[Double], LHdraftster: Option[Double], draftster: Option[Double])
  case class HitterFantasy(date: String, id: String, pitcherId: String, pitcherIndex: Int,
                           RHfanDuel: Option[Double], LHfanDuel: Option[Double], fanDuel: Option[Double],
                           RHdraftKings: Option[Double], LHdraftKings: Option[Double], draftKings: Option[Double],
                           RHdraftster: Option[Double], LHdraftster: Option[Double], draftster: Option[Double])

  case class BallparkDaily(var id: String, var date: String,
                           var RHhits: Int, var RHtotalBases: Int, var RHatBat: Int, var RHbaseOnBalls: Int, var RHhitByPitch: Int, var RHsacFly: Int,
                           var LHhits: Int, var LHtotalBases: Int, var LHatBat: Int, var LHbaseOnBalls: Int, var LHhitByPitch: Int, var LHsacFly: Int)
  case class Ballpark(id: String, name: String, aka: String, city: String, state: String, start: String, end: String, league: String, notes: String)

  case class IdMapping(mlbId: String, mlbName: String, mlbTeam: String, brefId: String, brefName: String, espnId: String, espnName: String, retroId: String, retroName: String)

  val ballparkDailiesTable = TableQuery[BallparkDailiesTable]
  val ballparkTable = TableQuery[BallparkTable]
  val fantasyPredictionTable = TableQuery[FantasyPredictionTable]
  val gameConditionsTable = TableQuery[GameConditionsTable]
  val gameOddsTable = TableQuery[GameOddsTable]
  val gameScoringTable = TableQuery[GameScoringTable]
  val gamedayScheduleTable = TableQuery[GamedayScheduleTable]
  val gamesTable = TableQuery[GamesTable]
  val hitterFantasyTable = TableQuery[HitterFantasyTable]
  val hitterFantasyMovingTable = TableQuery[HitterFantasyMovingTable]
  val hitterFantasyVolatilityTable = TableQuery[HitterFantasyVolatilityTable]
  val hitterMovingStats = TableQuery[HitterStatsMovingTable]
  val hitterRawLH = TableQuery[HitterRawLHStatsTable]
  val hitterRawRH = TableQuery[HitterRawRHStatsTable]
  val hitterStats = TableQuery[HitterDailyStatsTable]
  val hitterVolatilityStats = TableQuery[HitterStatsVolatilityTable]
  val idMappingTable = TableQuery[IdMappingTable]
  val injuryReportTable = TableQuery[InjuryReportTable]
  val pitcherDailyTable = TableQuery[PitcherDailyTable]
  val pitcherFantasy = TableQuery[PitcherFantasyTable]
  val pitcherFantasyMoving = TableQuery[PitcherFantasyMovingTable]
  val pitcherFantasyMovingStats = TableQuery[PitcherFantasyMovingTable]
  val pitcherFantasyStats = TableQuery[PitcherFantasyTable]
  val pitcherStats = TableQuery[PitcherDailyTable]
  val playersTable = TableQuery[PlayersTable]
  val teamsTable = TableQuery[TeamsTable]
}

import RealityballRecords._
object RealityballJsonProtocol extends DefaultJsonProtocol {
  import RealityballRecords._
  implicit val playerFormat = jsonFormat8(Player)
  implicit val playerSummaryFormat = jsonFormat8(PlayerSummary)
  implicit val pitcherSummaryFormat = jsonFormat9(PitcherSummary)
  implicit val playerDataFormat = jsonFormat2(PlayerData)
  implicit val pitcherDataFormat = jsonFormat2(PitcherData)
  implicit val teamFormat = jsonFormat12(Team)
  implicit val gamedayScheduleFormat = jsonFormat18(GamedaySchedule)
  implicit val gameOddsFormat = jsonFormat5(GameOdds)
  implicit val fullGameInfoFormat = jsonFormat2(FullGameInfo)
  implicit val injuryReportFormat = jsonFormat6(InjuryReport)
}

class TeamsTable(tag: Tag) extends Table[Team](tag, "teams") {
  def year = column[String]("year")
  def mnemonic = column[String]("mnemonic")
  def league = column[String]("league")
  def city = column[String]("city")
  def name = column[String]("name")
  def site = column[String]("site")
  def zipCode = column[String]("zipCode")
  def mlbComId = column[String]("mlbComId")
  def mlbComName = column[String]("mlbComName")
  def timeZone = column[String]("timeZone")
  def coversComId = column[String]("coversComId")
  def coversComName = column[String]("coversComName")

  def * = (year, mnemonic, league, city, name, site, zipCode, mlbComId, mlbComName, timeZone, coversComId, coversComName) <> (Team.tupled, Team.unapply)
}

class GamesTable(tag: Tag) extends Table[Game](tag, "games") {
  def id = column[String]("id", O.PrimaryKey)
  def homeTeam = column[String]("homeTeam")
  def visitingTeam = column[String]("visitingTeam")
  def site = column[String]("site")
  def date = column[String]("date")
  def number = column[Int]("number")
  def startingHomePitcher = column[String]("startingHomePitcher")
  def startingVisitingPitcher = column[String]("startingVisitingPitcher")

  def * = (id, homeTeam, visitingTeam, site, date, number, startingHomePitcher, startingVisitingPitcher) <> (Game.tupled, Game.unapply)
}

class GameConditionsTable(tag: Tag) extends Table[GameConditions](tag, "gameConditions") {
  def id = column[String]("id", O.PrimaryKey)
  def startTime = column[String]("startTime")
  def daynight = column[String]("daynight")
  def usedh = column[Boolean]("usedh")
  def temp = column[Int]("temp")
  def winddir = column[String]("winddir")
  def windspeed = column[Int]("windspeed")
  def fieldcond = column[String]("fieldcond")
  def precip = column[String]("precip")
  def sky = column[String]("sky")

  def * = (id, startTime, daynight, usedh, temp, winddir, windspeed, fieldcond, precip, sky) <> (GameConditions.tupled, GameConditions.unapply)
}

class GamedayScheduleTable(tag: Tag) extends Table[GamedaySchedule](tag, "gamedaySchedule") {
  def id = column[String]("id", O.PrimaryKey)
  def homeTeam = column[String]("homeTeam")
  def visitingTeam = column[String]("visitingTeam")
  def site = column[String]("site")
  def date = column[String]("date")
  def number = column[Int]("number")
  def result = column[String]("result")
  def winningPitcher = column[String]("winningPitcher")
  def losingPitcher = column[String]("losingPitcher")
  def record = column[String]("record")
  def startingHomePitcher = column[String]("startingHomePitcher")
  def startingVisitingPitcher = column[String]("startingVisitingPitcher")
  def startTime = column[String]("startTime")
  def temp = column[Int]("temp")
  def winddir = column[String]("winddir")
  def windspeed = column[Int]("windspeed")
  def precip = column[String]("precip")
  def sky = column[String]("sky")

  def * = (id, homeTeam, visitingTeam, site, date, number, result, winningPitcher, losingPitcher, record, startingHomePitcher, startingVisitingPitcher, startTime, temp, winddir, windspeed, precip, sky) <> (GamedaySchedule.tupled, GamedaySchedule.unapply)
}

class FantasyPredictionTable(tag: Tag) extends Table[FantasyPrediction](tag, "fantasyPrediction") {
  def id = column[String]("id")
  def gameId = column[String]("gameId")
  def productionRate = column[Option[Double]]("productionRate")
  def eFanduel = column[Option[Double]]("eFanduel")
  def eDraftKings = column[Option[Double]]("eDraftKings")
  def eDraftster = column[Option[Double]]("eDraftster")
  def fanduelBase = column[Option[Double]]("fanduelBase")
  def draftKingsBase = column[Option[Double]]("draftKingsBase")
  def draftsterBase = column[Option[Double]]("draftsterBase")
  def fanduelVol = column[Option[Double]]("fanduelVol")
  def draftKingsVol = column[Option[Double]]("draftKingsVol")
  def draftsterVol = column[Option[Double]]("draftsterVol")
  def pitcherAdj = column[Option[Double]]("pitcherAdj")
  def parkAdj = column[Option[Double]]("parkAdj")
  def baTrendAdj = column[Option[Double]]("baTrendAdj")
  def oddsAdj = column[Option[Double]]("oddsAdj")
  def matchupAdj = column[Option[Double]]("matchupAdj")

  def pk = index("pk_id_game", (id, gameId))

  def * = (id, gameId, productionRate, eFanduel, eDraftKings, eDraftster, fanduelBase, draftKingsBase, draftsterBase, fanduelVol, draftKingsVol, draftsterVol, pitcherAdj, parkAdj, baTrendAdj, oddsAdj, matchupAdj) <> (FantasyPrediction.tupled, FantasyPrediction.unapply)
}

class GameOddsTable(tag: Tag) extends Table[GameOdds](tag, "gameOdds") {
  def id = column[String]("id", O.PrimaryKey)
  def visitorML = column[Int]("visitorML")
  def homeML = column[Int]("homeML")
  def overUnder = column[Double]("overUnder")
  def overUnderML = column[Int]("overUnderML")

  def * = (id, visitorML, homeML, overUnder, overUnderML) <> (GameOdds.tupled, GameOdds.unapply)
}

class InjuryReportTable(tag: Tag) extends Table[InjuryReport](tag, "InjuryReport") {
  def mlbId = column[String]("mlbId")
  def reportTime = column[String]("reportTime")
  def injuryReportDate = column[String]("injuryReportDate")
  def status = column[String]("status")
  def dueBack = column[String]("dueBack")
  def injury = column[String]("injury")

  def * = (mlbId, reportTime, injuryReportDate, status, dueBack, injury) <> (InjuryReport.tupled, InjuryReport.unapply)
}

class BallparkDailiesTable(tag: Tag) extends Table[BallparkDaily](tag, "ballparkDailies") {
  def id = column[String]("id")
  def date = column[String]("date")
  def RHhits = column[Int]("RHhits")
  def RHtotalBases = column[Int]("RHtotalBases")
  def RHatBat = column[Int]("RHatBat")
  def RHbaseOnBalls = column[Int]("RHbaseOnBalls")
  def RHhitByPitch = column[Int]("RHhitByPitch")
  def RHsacFly = column[Int]("RHsacFly")
  def LHhits = column[Int]("LHhits")
  def LHtotalBases = column[Int]("LHtotalBases")
  def LHatBat = column[Int]("LHatBat")
  def LHbaseOnBalls = column[Int]("LHbaseOnBalls")
  def LHhitByPitch = column[Int]("LHhitByPitch")
  def LHsacFly = column[Int]("LHsacFly")

  def pk = index("pk_id_date", (id, date))

  def * = (id, date, RHhits, RHtotalBases, RHatBat, RHbaseOnBalls, RHhitByPitch, RHsacFly, LHhits, LHtotalBases, LHatBat, LHbaseOnBalls, LHhitByPitch, LHsacFly) <> (BallparkDaily.tupled, BallparkDaily.unapply)
}

class BallparkTable(tag: Tag) extends Table[Ballpark](tag, "ballpark") {
  def id = column[String]("id", O.PrimaryKey)
  def name = column[String]("name")
  def aka = column[String]("aka")
  def city = column[String]("city")
  def state = column[String]("state")
  def start = column[String]("start")
  def end = column[String]("end")
  def league = column[String]("league")
  def notes = column[String]("notes")

  def * = (id, name, aka, city, state, start, end, league, notes) <> (Ballpark.tupled, Ballpark.unapply)
}

class GameScoringTable(tag: Tag) extends Table[GameScoring](tag, "gameScoring") {
  def id = column[String]("id", O.PrimaryKey)
  def umphome = column[String]("umphome")
  def ump1b = column[String]("ump1b")
  def ump2b = column[String]("ump2b")
  def ump3b = column[String]("ump3b")
  def howscored = column[String]("howscored")
  def timeofgame = column[Int]("timeofgame")
  def attendance = column[Int]("attendarnce")
  def wp = column[String]("wp")
  def lp = column[String]("lp")
  def save = column[String]("save")

  def * = (id, umphome, ump1b, ump2b, ump3b, howscored, timeofgame, attendance, wp, lp, save) <> (GameScoring.tupled, GameScoring.unapply)
}

class PitcherDailyTable(tag: Tag) extends Table[PitcherDaily](tag, "pitcherDaily") {
  def id = column[String]("id");
  def game = column[String]("game");
  def date = column[String]("date")
  def daysSinceLastApp = column[Int]("daysSinceLastApp")
  def opposing = column[String]("opposing")

  def win = column[Int]("win")
  def loss = column[Int]("loss")
  def save = column[Int]("save")
  def hits = column[Int]("hits")
  def walks = column[Int]("walks")
  def hitByPitch = column[Int]("hitByPitch")
  def strikeOuts = column[Int]("strikeOuts")
  def groundOuts = column[Int]("groundOuts")
  def flyOuts = column[Int]("flyOuts")
  def earnedRuns = column[Int]("earnedRuns")
  def outs = column[Int]("outs")
  def shutout = column[Boolean]("shutout")
  def noHitter = column[Boolean]("noHitter")
  def pitches = column[Int]("pitches")
  def balls = column[Int]("balls")
  def style = column[String]("style")

  def pk = index("pk_id_date", (id, game)) // Duplicate issue with Joaquin Benoit on 20100910

  def * = (id, game, date, daysSinceLastApp, opposing, win, loss, save, hits, walks, hitByPitch, strikeOuts, groundOuts, flyOuts, earnedRuns, outs, shutout, noHitter, pitches, balls, style) <> (PitcherDaily.tupled, PitcherDaily.unapply)
}

class PlayersTable(tag: Tag) extends Table[Player](tag, "players") {
  def id = column[String]("id"); def year = column[String]("year");
  def lastName = column[String]("lastName")
  def firstName = column[String]("firstName")
  def batsWith = column[String]("batsWith")
  def throwsWith = column[String]("throwsWith")
  def team = column[String]("team")
  def position = column[String]("position")

  def pk = primaryKey("pk_id_date", (id, year))

  def * = (id, year, lastName, firstName, batsWith, throwsWith, team, position) <> (Player.tupled, Player.unapply)
}

class IdMappingTable(tag: Tag) extends Table[IdMapping](tag, "idMapping") {
  def mlbId = column[String]("mlbId"); def mlbName = column[String]("mlbName"); def mlbTeam = column[String]("mlbTeam")
  def brefId = column[String]("brefId"); def brefName = column[String]("brefName");
  def espnId = column[String]("espnId"); def espnName = column[String]("espnName");
  def retroId = column[String]("retroId"); def retroName = column[String]("retroName");

  def pk = primaryKey("pk_mlb_id", (mlbId))

  def * = (mlbId, mlbName, mlbTeam, brefId, brefName, espnId, espnName, retroId, retroName) <> (IdMapping.tupled, IdMapping.unapply)
}

class HitterRawLHStatsTable(tag: Tag) extends Table[(String, String, String, Int, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tag, "hitterRawLHstats") {
  def date = column[String]("date"); def id = column[String]("id");
  def gameId = column[String]("gameId"); def side = column[Int]("side");
  def pitcherId = column[String]("pitcherId"); def pitcherIndex = column[Int]("pitcherIndex")
  def LHatBat = column[Int]("LHatBat")
  def LHsingle = column[Int]("LHsingle")
  def LHdouble = column[Int]("LHdouble")
  def LHtriple = column[Int]("LHtriple")
  def LHhomeRun = column[Int]("LHhomeRun")
  def LHRBI = column[Int]("LHRBI")
  def LHruns = column[Int]("LHruns")
  def LHbaseOnBalls = column[Int]("LHbaseOnBalls")
  def LHhitByPitch = column[Int]("LHhitByPitch")
  def LHsacFly = column[Int]("LHsacFly")
  def LHsacHit = column[Int]("LHsacHit")
  def LHstrikeOut = column[Int]("LHstrikeOut")
  def LHflyBall = column[Int]("LHflyBall")
  def LHgroundBall = column[Int]("LHgroundBall")

  def pk = index("pk_id_date", (id, date)) // First game of double headers are ignored for now

  def * : ProvenShape[(String, String, String, Int, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)] =
    (date, id, gameId, side, pitcherId, pitcherIndex, LHatBat, LHsingle, LHdouble, LHtriple, LHhomeRun, LHRBI, LHruns, LHbaseOnBalls, LHhitByPitch, LHsacFly, LHsacHit, LHstrikeOut, LHflyBall, LHgroundBall)
}

class HitterRawRHStatsTable(tag: Tag) extends Table[(String, String, String, Int, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)](tag, "hitterRawRHstats") {
  def date = column[String]("date"); def id = column[String]("id");
  def gameId = column[String]("gameId"); def side = column[Int]("side");
  def pitcherId = column[String]("pitcherId"); def pitcherIndex = column[Int]("pitcherIndex")
  def RHatBat = column[Int]("RHatBat")
  def RHsingle = column[Int]("RHsingle")
  def RHdouble = column[Int]("RHdouble")
  def RHtriple = column[Int]("RHtriple")
  def RHhomeRun = column[Int]("RHhomeRun")
  def RHRBI = column[Int]("RHRBI")
  def RHruns = column[Int]("RHruns")
  def RHbaseOnBalls = column[Int]("RHbaseOnBalls")
  def RHhitByPitch = column[Int]("RHhitByPitch")
  def RHsacFly = column[Int]("RHsacFly")
  def RHsacHit = column[Int]("RHsacHit")
  def RHstrikeOut = column[Int]("RHstrikeOut")
  def RHflyBall = column[Int]("RHflyBall")
  def RHgroundBall = column[Int]("RHgroundBall")

  def pk = index("pk_id_date", (id, date)) // First game of double headers are ignored for now

  def * : ProvenShape[(String, String, String, Int, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)] =
    (date, id, gameId, side, pitcherId, pitcherIndex, RHatBat, RHsingle, RHdouble, RHtriple, RHhomeRun, RHRBI, RHruns, RHbaseOnBalls, RHhitByPitch, RHsacFly, RHsacHit, RHstrikeOut, RHflyBall, RHgroundBall)
}

class HitterDailyStatsTable(tag: Tag) extends Table[(String, String, String, Int, Int, Int, String, Int, Int, Int, Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])](tag, "hitterDailyStats") {

  def date = column[String]("date"); def id = column[String]("id");
  def gameId = column[String]("gameId"); def side = column[Int]("side");
  def lineupPosition = column[Int]("lineupPosition"); def lineupPositionRegime = column[Int]("lineupPositionRegime")
  def pitcherId = column[String]("pitcherId"); def pitcherIndex = column[Int]("pitcherIndex")
  def atBats = column[Int]("ab"); def plateAppearances = column[Int]("pa")
  def RHdailyBattingAverage = column[Option[Double]]("RHdailyBattingAverage")
  def LHdailyBattingAverage = column[Option[Double]]("LHdailyBattingAverage")
  def dailyBattingAverage = column[Option[Double]]("dailyBattingAverage")
  def RHbattingAverage = column[Option[Double]]("RHbattingAverage")
  def LHbattingAverage = column[Option[Double]]("LHbattingAverage")
  def battingAverage = column[Option[Double]]("battingAverage")
  def RHonBasePercentage = column[Option[Double]]("RHonBasePercentage")
  def LHonBasePercentage = column[Option[Double]]("LHonBasePercentage")
  def onBasePercentage = column[Option[Double]]("onBasePercentage")
  def RHsluggingPercentage = column[Option[Double]]("RHsluggingPercentage")
  def LHsluggingPercentage = column[Option[Double]]("LHsluggingPercentage")
  def sluggingPercentage = column[Option[Double]]("sluggingPercentage")

  def pk = index("pk_id_date", (id, date)) // First game of double headers are ignored for now
  def gameIdIndex = index("gameId", (gameId))

  def * = (date, id, gameId, side, lineupPosition, lineupPositionRegime, pitcherId, pitcherIndex, atBats, plateAppearances,
    RHdailyBattingAverage, LHdailyBattingAverage, dailyBattingAverage,
    RHbattingAverage, LHbattingAverage, battingAverage,
    RHonBasePercentage, LHonBasePercentage, onBasePercentage,
    RHsluggingPercentage, LHsluggingPercentage, sluggingPercentage)
}

class HitterStatsMovingTable(tag: Tag) extends Table[HitterStatsMoving](tag, "hitterMovingStats") {

  def date = column[String]("date"); def id = column[String]("id");
  def pitcherId = column[String]("pitcherId"); def pitcherIndex = column[Int]("pitcherIndex")
  def RHbattingAverageMov = column[Option[Double]]("RHbattingAverageMov")
  def LHbattingAverageMov = column[Option[Double]]("LHbattingAverageMov")
  def battingAverageMov = column[Option[Double]]("battingAverageMov")
  def RHonBasePercentageMov = column[Option[Double]]("RHonBasePercentageMov")
  def LHonBasePercentageMov = column[Option[Double]]("LHonBasePercentageMov")
  def onBasePercentageMov = column[Option[Double]]("onBasePercentageMov")
  def RHsluggingPercentageMov = column[Option[Double]]("RHsluggingPercentageMov")
  def LHsluggingPercentageMov = column[Option[Double]]("LHsluggingPercentageMov")
  def sluggingPercentageMov = column[Option[Double]]("sluggingPercentageMov")
  def RHstyle = column[String]("RHstyle")
  def LHstyle = column[String]("LHstyle")
  def style = column[String]("style")

  def pk = index("pk_id_date", (id, date))

  def * = (date, id, pitcherId, pitcherIndex,
    RHbattingAverageMov, LHbattingAverageMov, battingAverageMov,
    RHonBasePercentageMov, LHonBasePercentageMov, onBasePercentageMov,
    RHsluggingPercentageMov, LHsluggingPercentageMov, sluggingPercentageMov,
    RHstyle, LHstyle, style) <> (HitterStatsMoving.tupled, HitterStatsMoving.unapply)
}

class HitterFantasyTable(tag: Tag) extends Table[HitterFantasyDaily](tag, "hitterFantasyStats") {

  def date = column[String]("date"); def id = column[String]("id");
  def gameId = column[String]("gameId"); def side = column[Int]("side");
  def pitcherId = column[String]("pitcherId"); def pitcherIndex = column[Int]("pitcherIndex")
  def productionRate = column[Option[Double]]("productionRate")
  def RHfanDuel = column[Option[Double]]("RHfanDuel")
  def LHfanDuel = column[Option[Double]]("LHfanDuel")
  def fanDuel = column[Option[Double]]("fanDuel")
  def RHdraftKings = column[Option[Double]]("RHdraftKings")
  def LHdraftKings = column[Option[Double]]("LHdraftKings")
  def draftKings = column[Option[Double]]("draftKings")
  def RHdraftster = column[Option[Double]]("RHdraftster")
  def LHdraftster = column[Option[Double]]("LHdraftster")
  def draftster = column[Option[Double]]("draftster")

  def pk = index("pk_id_date", (id, date))
  def gameIndex = index("gameId_id", (gameId, id))

  def * = (date, id, gameId, side,
    pitcherId, pitcherIndex, productionRate,
    RHfanDuel, LHfanDuel, fanDuel,
    RHdraftKings, LHdraftKings, draftKings,
    RHdraftster, LHdraftster, draftster) <> (HitterFantasyDaily.tupled, HitterFantasyDaily.unapply)
}

class HitterFantasyMovingTable(tag: Tag) extends Table[HitterFantasy](tag, "hitterFantasyMovingStats") {

  def date = column[String]("date"); def id = column[String]("id");
  def pitcherId = column[String]("pitcherId"); def pitcherIndex = column[Int]("pitcherIndex")
  def RHfanDuel = column[Option[Double]]("RHfanDuel")
  def LHfanDuel = column[Option[Double]]("LHfanDuel")
  def fanDuel = column[Option[Double]]("fanDuel")
  def RHdraftKings = column[Option[Double]]("RHdraftKings")
  def LHdraftKings = column[Option[Double]]("LHdraftKings")
  def draftKings = column[Option[Double]]("draftKings")
  def RHdraftster = column[Option[Double]]("RHdraftster")
  def LHdraftster = column[Option[Double]]("LHdraftster")
  def draftster = column[Option[Double]]("draftster")

  def pk = index("pk_id_date", (id, date))

  def * = (date, id, pitcherId, pitcherIndex,
    RHfanDuel, LHfanDuel, fanDuel,
    RHdraftKings, LHdraftKings, draftKings,
    RHdraftster, LHdraftster, draftster) <> (HitterFantasy.tupled, HitterFantasy.unapply)
}

class HitterFantasyVolatilityTable(tag: Tag) extends Table[HitterFantasy](tag, "hitterFantasyVolatilityStats") {

  def date = column[String]("date"); def id = column[String]("id");
  def pitcherId = column[String]("pitcherId"); def pitcherIndex = column[Int]("pitcherIndex")
  def RHfanDuel = column[Option[Double]]("RHfanDuel")
  def LHfanDuel = column[Option[Double]]("LHfanDuel")
  def fanDuel = column[Option[Double]]("fanDuel")
  def RHdraftKings = column[Option[Double]]("RHdraftKings")
  def LHdraftKings = column[Option[Double]]("LHdraftKings")
  def draftKings = column[Option[Double]]("draftKings")
  def RHdraftster = column[Option[Double]]("RHdraftster")
  def LHdraftster = column[Option[Double]]("LHdraftster")
  def draftster = column[Option[Double]]("draftster")

  def pk = index("pk_id_date", (id, date))

  def * = (date, id, pitcherId, pitcherIndex,
    RHfanDuel, LHfanDuel, fanDuel,
    RHdraftKings, LHdraftKings, draftKings,
    RHdraftster, LHdraftster, draftster) <> (HitterFantasy.tupled, HitterFantasy.unapply)
}

class HitterStatsVolatilityTable(tag: Tag) extends Table[(String, String, String, Int, Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double], Option[Double])](tag, "hitterVolatilityStats") {

  def date = column[String]("date"); def id = column[String]("id");
  def pitcherId = column[String]("pitcherId"); def pitcherIndex = column[Int]("pitcherIndex")
  def RHbattingVolatility = column[Option[Double]]("RHbattingVolatility")
  def LHbattingVolatility = column[Option[Double]]("LHbattingVolatility")
  def battingVolatility = column[Option[Double]]("battingVolatility")
  def RHonBaseVolatility = column[Option[Double]]("RHonBaseVolatility")
  def LHonBaseVolatility = column[Option[Double]]("LHonBaseVolatility")
  def onBaseVolatility = column[Option[Double]]("onBaseVolatility")
  def RHsluggingVolatility = column[Option[Double]]("RHsluggingVolatility")
  def LHsluggingVolatility = column[Option[Double]]("LHsluggingVolatility")
  def sluggingVolatility = column[Option[Double]]("sluggingVolatility")

  def pk = index("pk_id_date", (id, date))

  def * = (date, id, pitcherId, pitcherIndex,
    RHbattingVolatility, LHbattingVolatility, battingVolatility,
    RHonBaseVolatility, LHonBaseVolatility, onBaseVolatility,
    RHsluggingVolatility, LHsluggingVolatility, sluggingVolatility)
}

class PitcherFantasyTable(tag: Tag) extends Table[(String, String, Option[Double], Option[Double], Option[Double])](tag, "pitcherFantasyStats") {

  def date = column[String]("date"); def id = column[String]("id");
  def fanDuel = column[Option[Double]]("fanDuel")
  def draftKings = column[Option[Double]]("draftKings")
  def draftster = column[Option[Double]]("draftster")

  def pk = index("pk_id_date", (id, date))

  def * = (date, id, fanDuel, draftKings, draftster)
}

class PitcherFantasyMovingTable(tag: Tag) extends Table[(String, String, Option[Double], Option[Double], Option[Double])](tag, "pitcherFantasyMovingStats") {

  def date = column[String]("date"); def id = column[String]("id");
  def fanDuelMov = column[Option[Double]]("fanDuelMov")
  def draftKingsMov = column[Option[Double]]("draftKingsMov")
  def draftsterMov = column[Option[Double]]("draftsterMov")

  def pk = index("pk_id_date", (id, date))

  def * = (date, id, fanDuelMov, draftKingsMov, draftsterMov)
}
