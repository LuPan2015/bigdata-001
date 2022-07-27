package com.bigdata.model;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;

/**
 * @author lupan on 2022-07-27
 */
@Getter
@Setter
public class DataEvent {
    private String connector;
    private String db;
    private String table;
    private JSONObject data;
}
