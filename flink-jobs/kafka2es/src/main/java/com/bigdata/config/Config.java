package com.bigdata.config;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lupan on 2022-07-12
 */
@Setter
@Getter
@Slf4j
public class Config {
    private Integer checkpointInterval;
    private String kafkaBootstrapServers;
    private String kafkaSourceTopics;
    private String kafkaConsumeGroupIp;
    private String kafkaSourceName;
    private String esServer;
    private Integer esPort = 9200;
    private String kafkaSourceId;
    private String mainStreamName;
    private String fields;
    private String esClusterName;
    private String orcUrl;
    private String aiUrl;
    private String contentUrl;
}
