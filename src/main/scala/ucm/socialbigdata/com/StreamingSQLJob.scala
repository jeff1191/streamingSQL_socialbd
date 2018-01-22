package ucm.socialbigdata.com

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.net.{InetAddress, InetSocketAddress}
import java.util
import java.util.Date

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSink, ElasticsearchSinkFunction}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.ExpressionParser
import org.apache.flink.types.Row
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import ucm.socialbigdata.com.config.SocialBDProperties
import ucm.socialbigdata.com.elasticsearch.SimpleElasticsearchSink
import ucm.socialbigdata.com.operations.RowToJSONMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
object StreamingSQLJob {

  case class Air(estacion: String,
                 magnitud: String,
                 tecnica: String,
                 horario: String,
                 fecha: String,
                 listaHoras: List[GroupHour])

  case class GroupHour(hora:String, valor:String, isValid:String)

  def main(args: Array[String]) {


    val socialBDProperties = new SocialBDProperties(args(0))


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env);

    val elem1 = Air("est1","mag1","tec1","horario1",new Date().toString, List(GroupHour("hora1", "value1", "true"),GroupHour("hora1.2", "value1.2", "true")))
    val elem2 = Air("est2","mag2","tec2","horario2",new Date().toString, List(GroupHour("hora2", "value2", "true"),GroupHour("hora2.2", "value2.2", "true")))
    val elem3 = Air("est3","mag3","tec3","horario3",new Date().toString, List(GroupHour("hora3", "value3", "true"),GroupHour("hora3.2", "value3.2", "true")))


    val airCollection = List(elem1, elem2, elem3)


    val originDataStream = env.fromCollection(airCollection.toSeq)

    tableEnv.registerDataStream(socialBDProperties.topic, originDataStream, 'estacion , 'magnitud, 'tecnica, 'horario, 'fecha, 'listaHoras)

    val query = socialBDProperties.query.replace("SELECT","select").replace("Select","select").replace("FROM","from").replace("From","from")


    val pattern = """select(.*)from""".r


    var fields = ""

    pattern.findAllIn(query).matchData foreach {
      m => fields = m.group(1)
    }

    val resultTable = tableEnv.sqlQuery(query).toRetractStream[Row]

    val jsonDataStream : DataStream[String] = resultTable.map(_._2).map(new RowToJSONMap(fields.split(",").toList))

    jsonDataStream.print()
    val config = getElasticConfiguration(socialBDProperties)

//    val transports = new java.util.ArrayList[InetSocketAddress]
//    transports.add(new InetSocketAddress(InetAddress.getByName(socialBDProperties.elasticUrl), socialBDProperties.elasticPort))

    jsonDataStream.addSink(new ElasticsearchSink[String](config,
      new SimpleElasticsearchSink(socialBDProperties.elasticIndex, socialBDProperties.elasticType)))
    // execute program
    env.execute("Flink SQL SocialBigData-CM")
  }

  def getElasticConfiguration(socialBDProperties: SocialBDProperties): util.HashMap[String, String] = {
    val config = new java.util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", socialBDProperties.elasticClusterName)
    config.put("node.name", socialBDProperties.elasticNodeName)
    config
  }
}