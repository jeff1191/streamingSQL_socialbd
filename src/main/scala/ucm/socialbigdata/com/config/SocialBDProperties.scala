package ucm.socialbigdata.com.config

import java.io.File

import com.typesafe.config.ConfigFactory
/**
  * Created by Jeff on 16/04/2017.
  */
@SerialVersionUID(100L)
class SocialBDProperties(path:String) extends Serializable{

  private val conf = ConfigFactory.parseFile(new File(path))

  //checks about if exist components in application.conf
  conf.checkValid(ConfigFactory.defaultReference(), "kafkaBrokersUrls")
  conf.checkValid(ConfigFactory.defaultReference(), "zkUrl")
  conf.checkValid(ConfigFactory.defaultReference(), "elasticClusterName")
  conf.checkValid(ConfigFactory.defaultReference(), "elasticNodeName")
  conf.checkValid(ConfigFactory.defaultReference(), "elasticPort")
  conf.checkValid(ConfigFactory.defaultReference(), "elasticUrl")
  conf.checkValid(ConfigFactory.defaultReference(), "query")
  conf.checkValid(ConfigFactory.defaultReference(), "elasticIndex")
  conf.checkValid(ConfigFactory.defaultReference(), "elasticType")
  conf.checkValid(ConfigFactory.defaultReference(), "topic")


  val kafkaBrokersUrls =  conf.getString("kafkaBrokersUrls")
  val zkUrl =  conf.getString("zkUrl")
  val elasticClusterName =  conf.getString("elasticClusterName")
  val elasticNodeName =  conf.getString("elasticNodeName")
  val elasticPort =  conf.getInt("elasticPort")
  val elasticUrl =  conf.getString("elasticUrl")
  val query =  conf.getString("query")
  val elasticIndex =  conf.getString("elasticIndex")
  val elasticType =  conf.getString("elasticType")
  val topic =  conf.getString("topic")


  override def toString = s"SocialBDProperties($conf, $kafkaBrokersUrls, $zkUrl, " +
    s"$elasticClusterName, $elasticNodeName, $elasticPort, $elasticUrl, $query, " +
    s"$elasticIndex, $elasticType, $topic)"
}
