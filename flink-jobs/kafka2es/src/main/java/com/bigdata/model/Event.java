package com.bigdata.model;

import lombok.Getter;
import lombok.Setter;

/**
 * @author lupan on 2022-07-12
 */
@Getter
@Setter
public class Event {
    private String indexName;
    private String type;
    private String data;
    private String id;
}
