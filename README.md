# Usage

## 配置

配置文件位于`src/main/resources/hcbConfig.properties`

```properties
#spark任务带状态操作符个数
ops=3
#Zookeeper servers
zkServers=192.168.225.6:2181,192.168.225.6:2182,192.168.225.6:2183
#Kafka brokers
kafkaServers=192.168.225.6:9092,192.168.225.6:9093,192.168.225.6:9094
#运行日志路径
logPath=./run.log
```

## Package

执行`ex.sh`,将生成的`target/hcbMfs-1.0.0.jar`放在spark任务的依赖中.

## 使用示例代码

```scala
val dataStreamWriter = wordcount
      .writeStream
      .queryName("kafka_test2")
			// localhost替换为kafka的一个broker ip
      .option("checkpointLocation","mfs://localhost:8888/checkpointRoot")
      .outputMode(OutputMode.Complete())
      .format("console")

val query = dataStreamWriter.start()
query.awaitTermination()
```

