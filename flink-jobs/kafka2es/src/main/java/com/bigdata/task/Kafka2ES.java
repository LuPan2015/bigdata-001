package com.bigdata.task;

import com.bigdata.config.Config;
import com.bigdata.map.HDFS2FastDFSMapFunction;
import com.bigdata.map.ProcessFunction;
import com.bigdata.model.DataEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @author lupan on 2022-07-12
 */
@Slf4j
public class Kafka2ES {
    public static void main(String[] args) throws Exception {
        log.info("elasticSearch Sink start");
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
        //env.getCheckpointConfig().setCheckpointingMode(RETAIN_ON_CANCELLATION);
        env.enableCheckpointing(config.getCheckpointInterval(), CheckpointingMode.EXACTLY_ONCE);

        Pattern pattern1 = Pattern.compile("JG([-.\\w])+");
        // 创建 Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopicPattern(pattern1)
                .setGroupId(config.getKafkaConsumeGroupIp())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 从 Kafka Source 中获得数据流
        DataStream<String> kafkaSource = env.fromSource(source,
                WatermarkStrategy.noWatermarks(), config.getKafkaSourceName())
                .name(config.getKafkaSourceId())
                .uid(config.getKafkaSourceId());

        // 解析原始数据
        SingleOutputStreamOperator<DataEvent> mainDataStream =  kafkaSource
                .process(new ProcessFunction())
                .name(config.getMainStreamName())
                .uid(config.getMainStreamName())
                .setParallelism(1);

        // 数据上传至 gofastdfs
        mainDataStream.flatMap(new HDFS2FastDFSMapFunction(config.getFields()));

        Map<String, String> esConfig = new HashMap<>();
        esConfig.put("cluster.name", config.getEsClusterName());
        esConfig.put("bulk.flush.max.actions", "1");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName(config.getEsServer()), config.getEsPort()));

        mainDataStream.addSink(new ElasticsearchSink<DataEvent>(esConfig,transportAddresses,
                new ElasticsearchSinkFunction<DataEvent>(){
                    public IndexRequest createIndexRequest(DataEvent event) {
                        String type = "docs";
                        String index = event.getConnector()+"-"+event.getDb()+"-"+event.getTable();
                        Map<String, Object> json = event.getData();
                        return Requests.indexRequest()
                            .index(index)
                            .type(type)
                            .source(json);
                    }

                    @Override
                    public void process(DataEvent dataEvent, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(dataEvent));
                    }
                }));
        final String jobName = "es Sink";
        env.execute(jobName);
    }
}
