package org.sia.util

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

case class KafkaProducerWrapper(brockerList: String){

  val producerProp={
    val props= new java.util.Properties
    props.put("metadata.broker.list",brockerList)
    props
  }

  val p= new Producer[Array[Byte] ,Array[Byte]](
    new ProducerConfig(producerProp)
  )

  def send (topic: String, key:String, value: String): Unit = {
    p.send(new KeyedMessage(topic,key.toCharArray.map(_.toByte),
      value.toCharArray.map(_.toByte)))
  }

}

object KafkaProducerWrapper{

  var brokerList=""

  lazy val instance=new KafkaProducerWrapper(brokerList)

}
