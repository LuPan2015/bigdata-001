package com.bigdata.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @author lupan on 2022-07-12
 */
@Slf4j
public class Kafka2ESAndNeo4j {
    public static void main(String[] args) throws Exception {
        log.info("Clickhouse Sink start");

        //ClickhouseSinkConfig clickhouseSinkConfig = new ClickhouseSinkConfig("clickhouse-sink.yml");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(clickhouseSinkConfig.getEventCollectParallelism());

        // 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
       // env.enableCheckpointing(clickhouseSinkConfig.getCheckpointInterval(), CheckpointingMode.EXACTLY_ONCE);

        // 创建 Kafka Source
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers(clickhouseSinkConfig.getKafkaBootstrapServers())
//                .setTopics(clickhouseSinkConfig.getKafkaSourceTopics())
//                .setGroupId(clickhouseSinkConfig.getKafkaConsumeGroupIp())
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();

        // 从 Kafka Source 中获得数据流
//        DataStream<String> kafkaSource = env.fromSource(source,
//                WatermarkStrategy.noWatermarks(), clickhouseSinkConfig.getKafkaSourceName())
//                .name(clickhouseSinkConfig.getKafkaSourceId())
//                .uid(clickhouseSinkConfig.getKafkaSourceId());

        // 执行 ClickhouseFunction
//        SingleOutputStreamOperator<String> mainDataStream = kafkaSource
//                .process(new ClickhouseSinkFunction(ClickhouseSinkOptions.builder()
//                        .withUrl(clickhouseSinkConfig.getClickhouseUrl())
//                        .withUserName(clickhouseSinkConfig.getClickhouseUsername())
//                        .withPassword(clickhouseSinkConfig.getClickhousePassword())
//                        .build()))
//                .setParallelism(clickhouseSinkConfig.getEventCollectParallelism())
//                .name(clickhouseSinkConfig.getMainDataStreamName())
//                .uid(clickhouseSinkConfig.getMainDataStreamName());

        final String jobName = "Clickhouse Sink";
        env.execute(jobName);
    }
}
