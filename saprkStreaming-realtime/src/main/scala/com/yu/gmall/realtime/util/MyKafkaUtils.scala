package com.yu.gmall.realtime.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

object MyKafkaUtils {

  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    //kafka bootstrap
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),
    //group id
    //kv 反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //offset 提交/重置
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //default 5000ms
    //    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000"
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )

  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaDStream
  }

  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offsets))
    kafkaDStream
  }

  val producer: KafkaProducer[String, String] = createProducer()

  def createProducer(): KafkaProducer[String, String] = {
    val productConfigs: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]
    //    productConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    //    productConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils("kafka.bootstrap-servers"))
    productConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    productConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    productConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // 幂等
    productConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    //acks
    productConfigs.put(ProducerConfig.ACKS_CONFIG, "all")
    //batch.size 16kb
    //linger.ms 0
    //retries 0

    val producer = new KafkaProducer[String, String](productConfigs)
    producer
  }

  def close() = {
    if (producer != null)
      producer.close()
  }

  def send(topic: String, msg: String) = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  def send(topic: String, key: String, msg: String) = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  def flush(): Unit = {
    producer.flush();
  }

}
