package com.bigdata.task;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.config.Config;
import com.bigdata.map.ProcessFunction;
import com.bigdata.map.TransformFunction;
import com.bigdata.model.DataEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

        //Pattern pattern1 = Pattern.compile("JG([-.\\w])+");
        // 创建 Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getKafkaSourceTopics())
                //.setTopicPattern(pattern1)
                .setGroupId(""+System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // 从 Kafka Source 中获得数据流
        DataStream<String> kafkaSource = env.fromSource(source,
                WatermarkStrategy.noWatermarks(), config.getKafkaSourceName())
                .name(config.getKafkaSourceId())
                .uid(config.getKafkaSourceId());
        kafkaSource.print();


        SingleOutputStreamOperator<DataEvent> mainDataStream = kafkaSource
                .map(new TransformFunction())
                .name("mainDataStream")
                .uid("mainDataStreamUid")
                .setParallelism(1);

        // 数据上传至 gofastdfs
        SingleOutputStreamOperator sideStream = mainDataStream.process(new ProcessFunction(config));

        // 获取需要写 kafka 的 datastream
        OutputTag<DataEvent> outputKafka = new OutputTag<DataEvent>("kafka"){};
        DataStream<String> kafkaDataStream = sideStream.getSideOutput(outputKafka);
        sinkToKafka(config,kafkaDataStream);

        // 获取需要写 es 的 datastream
        OutputTag<DataEvent> outputEs = new OutputTag<DataEvent>("es"){};
        DataStream<DataEvent> esDataStream = sideStream.getSideOutput(outputEs);
        sinkToES(config,esDataStream);

        final String jobName = "es Sink";
        env.execute(jobName);
    }

    private static void sinkToKafka(Config config, DataStream<String> kafkaDataStream) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.getKafkaBootstrapServers());
        String targetTopic = "target-topic";
        KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(
                        targetTopic, // target topic
                        element.getBytes(StandardCharsets.UTF_8)); // record contents
            }
        };
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                targetTopic,             // target topic
                serializationSchema,    // serialization schema
                properties,             // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance
        kafkaDataStream.addSink(myProducer);
    }

    /**
     * 这里可以封装一个 esSink
     * @param config
     * @param esDataStream
     * @throws UnknownHostException
     */
    private static void sinkToES(Config config, DataStream<DataEvent> esDataStream) throws UnknownHostException {
        // 将 es 流中的数据写 es
        Map<String, String> esConfig = new HashMap<>();
        esConfig.put("cluster.name", config.getEsClusterName());
        esConfig.put("bulk.flush.max.actions", "1");
        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName(config.getEsServer()), config.getEsPort()));
        esDataStream.addSink(new ElasticsearchSink<DataEvent>(esConfig,transportAddresses,
                new ElasticsearchSinkFunction<DataEvent>(){
                    public IndexRequest createIndexRequest(DataEvent event) {
                        String type = "docs";
                        String index = event.getIndexName();
                        Map<String, Object> json = JSONObject.parseObject(event.getData());
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
    }
}
