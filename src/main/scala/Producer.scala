
import java.util.Properties

import org.apache.kafka.common.KafkaException
import kafka.producer.{KeyedMessage, Producer => KafkaProducer, ProducerConfig}
import kafka.utils.{ZkUtils, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import scala.util.parsing.json.{JSONObject, JSON, JSONType}

object Producer {
  val zkHost = "localhost"
  lazy val zkClient = new ZkClient(zkHost, 1000, 1000, ZKStringSerializer)

  val props = new Properties
  props.put("zookeeper.connect", s"${zkHost}")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("metadata.broker.list", getBrokerLists.mkString(","))

  val config = new ProducerConfig(props)
  lazy val producer = new KafkaProducer[String, String](config)


  def main(args : Array[String]) = {
    send("test", "Message")
  }


  def send(topic: String, message: String) = {
    val key = new KeyedMessage[String, String](topic, message)
    producer.send(key)
  }

  def getBrokerLists: List[String] = {
    val listIds = ZkUtils.getChildren(zkClient, "/brokers/ids").toList
    listIds.map { id =>
      val path = s"/brokers/ids/${id}"
      if (zkClient.exists(path)) {
        val result = JSON.parseRaw(ZkUtils.readData(zkClient, path)._1)
        val jo = result.get.asInstanceOf[JSONObject]
        val map = jo.obj.asInstanceOf[Map[String, String]]
        val (host, port) = (map.get("host"), map.get("port"))
        s"${host.get}:${port.get}".stripSuffix(".0")
      } else {
        throw new KafkaException("Broker with ID ${brokerId} doesn't exist")
      }
    }
  }
}
