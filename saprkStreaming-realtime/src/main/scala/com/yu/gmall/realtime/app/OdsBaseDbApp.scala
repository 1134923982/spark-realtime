package com.yu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.yu.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util

object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicName: String = "ODS_BASE_DB_1018"
    val groupId = "ODS_BASE_DB_GROUP_1018"
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null;
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val jsonObjDstream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        val dataJson: String = consumerRecord.value()
        val json: JSONObject = JSON.parseObject(dataJson)
        json
      }
    )
//    jsonObjDstream.print(100)

    jsonObjDstream.foreachRDD(rdd=>{
      // 事实表 广播变量
      val redisFactKeys = "FACT:TABLES"
      val jedis: Jedis = MyRedisUtils.getJedisFromPool()
      val factTables: util.Set[String] = jedis.smembers(redisFactKeys)
      val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
      //维度表 广播变量
      val redisDIMKeys = "DIM:TABLES"
      val dimTables: util.Set[String] = jedis.smembers(redisDIMKeys)
      val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
      println(factTables)
      println(dimTables)
      jedis.close()

      rdd.foreachPartition(jsonObjIter =>{
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (jsonObj <- jsonObjIter) {
          val operType: String = jsonObj.getString("type")
          val opValue: String = operType match {
            case "insert" => "I"
            case "update" => "U"
            case "delete" => "D"
            case "bootstrap-insert" => "I"
            case _ => null
          }
          if(opValue!=null){
            val tableName: String = jsonObj.getString("table")
            if (factTablesBC.value.contains(tableName)){
              val data: String = jsonObj.getString("data")
              val dwdTopicName:String = s"DWD_${tableName.toUpperCase}_${opValue}_1018"
              MyKafkaUtils.send(dwdTopicName, data)
            }
            if(dimTablesBC.value.contains(tableName)){
              val data: JSONObject = jsonObj.getJSONObject("data")
              val id: String = data.getString("id")
              val redisKey:String = s"DIM:${tableName.toUpperCase}:$id"
              jedis.set(redisKey, data.toJSONString)
            }
          }
        }
        jedis.close()
        MyKafkaUtils.flush()
      })
      MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
