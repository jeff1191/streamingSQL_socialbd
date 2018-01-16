package ucm.socialbigdata.com.operations

import net.liftweb.json.Extraction._
import net.liftweb.json.JsonAST._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row

class RowToJSONMap(fieldsList: List[String]) extends RichMapFunction[Row, String] {
  override def open(parameters: Configuration): Unit = super.open(parameters)

  def map(row: Row):String = {
    implicit val formats = net.liftweb.json.DefaultFormats
    var toConvert = scala.collection.mutable.Map[String, String]()

    fieldsList.zipWithIndex.foreach{ case (e, i) => toConvert += (e.trim -> row.getField(i).toString) }

    compactRender(decompose(toConvert.toMap))
  }
}