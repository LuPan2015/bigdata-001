package com.bigdata.task;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.config.Config;
import com.bigdata.map.ElasticSearchFunction;
import com.bigdata.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch6SinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.recycler.Recycler;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @author lupan on 2022-07-12
 */
@Slf4j
public class Kafka2ES {
    public static void main(String[] args) throws Exception {
        log.info("Clickhouse Sink start");
        Config config = new Config();
        // 解析 yaml 文件
        URL taskFileURL = Thread.currentThread().getContextClassLoader().getResource("config.yml");
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            config = mapper.readValue(taskFileURL,Config.class);
        } catch (IOException e) {
            log.error(e.getMessage());
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        env.enableCheckpointing(config.getCheckpointInterval(), CheckpointingMode.EXACTLY_ONCE);

        Pattern pattern1 = Pattern.compile("JG([-.\\w])+");
        // 创建 Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                //.setTopics("JG-k17.searchback.t_roles")
                .setTopicPattern(pattern1)
                //.setTopics(config.getKafkaSourceTopics())
                .setGroupId(config.getKafkaConsumeGroupIp())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 从 Kafka Source 中获得数据流
        DataStream<String> kafkaSource = env.fromSource(source,
                WatermarkStrategy.noWatermarks(), config.getKafkaSourceName())
                .name(config.getKafkaSourceId())
                .uid(config.getKafkaSourceId());
        // 数据处理
        SingleOutputStreamOperator<Event> mainDataStream =  kafkaSource
                .process(new ElasticSearchFunction())
                .name(config.getMainStreamName())
                .uid(config.getMainStreamName())
                .setParallelism(1);
        // 写入 es
        mainDataStream.sinkTo(
                new Elasticsearch6SinkBuilder<Event>()
                        .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                        .setHosts(new HttpHost(config.getEsServer(), config.getEsPort(), "http"))
                        .setEmitter(
                                (event, context, indexer) ->
                                        indexer.add(createIndexRequest(event)))
                        .build());
        //kafkaSource.print().setParallelism(1);
        final String jobName = "es Sink";
        env.execute(jobName);
    }

    private static IndexRequest createIndexRequest(Event event) {
        Map<String, Object> json = JSONObject.parseObject(event.getData());
        //json.put("data", event.getData());

        return Requests.indexRequest()
                .index(event.getIndexName())
                .type(event.getType())
                //.id(element)
                .source(json);
    }

}
