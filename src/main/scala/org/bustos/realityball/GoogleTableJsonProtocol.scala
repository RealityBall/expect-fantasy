package org.bustos.realityball

import spray.json._
import DefaultJsonProtocol._

class GoogleCell(val v: Any) {}
class GoogleColumn(val id: String, val label: String, val typeName: String) {}
class GoogleTooltipColumn() extends GoogleColumn("", "", "") {}

object GoogleCellJsonProtocol extends DefaultJsonProtocol {

  implicit object GoogleCellFormat extends RootJsonFormat[GoogleCell] {
    def write(c: GoogleCell) = c.v match {
      case x: String => JsObject("v" -> JsString(x))
      case x: Int    => JsObject("v" -> JsNumber(x))
      case x: Double => JsObject("v" -> JsNumber(x))
      // TODO: Handle other basic types (e.g. Date)
    }
    def read(value: JsValue) = value match {
      case _ => deserializationError("Undefined Read")
      // TODO: Provide read functionality
    }
  }

  implicit object GoogleColumnFormat extends RootJsonFormat[GoogleColumn] {
    def write(c: GoogleColumn) = {
      c match {
        case x: GoogleTooltipColumn => JsObject("type" -> JsString("string"), "role" -> JsString("tooltip"), "p" -> JsObject("html" -> JsBoolean(true)))
        case _ => JsObject(
          "id" -> JsString(c.id),
          "label" -> JsString(c.label),
          "type" -> JsString(c.typeName) // Required because `type' is a reserved word in Scala
          )
      }
    }
    def read(value: JsValue) = value match {
      case _ => deserializationError("Undefined Read")
      // TODO: Provide read functionality
    }
  }
}

object GoogleTableJsonProtocol extends DefaultJsonProtocol {

  import GoogleCellJsonProtocol._

  case class GoogleRow(c: List[GoogleCell])
  case class GoogleTable(cols: List[GoogleColumn], rows: List[GoogleRow])

  implicit val googleRowJSON = jsonFormat1(GoogleRow)
  implicit val googleTableJSON = jsonFormat2(GoogleTable)
}
