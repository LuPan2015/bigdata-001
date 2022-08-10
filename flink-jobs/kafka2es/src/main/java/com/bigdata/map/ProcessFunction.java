package com.bigdata.map;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.model.DataEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.Collector;

/**
 * @author lupan on 2022-07-12
 */
@Slf4j
public class ProcessFunction extends org.apache.flink.streaming.api.functions.ProcessFunction<String, DataEvent> {
    @Override
    public void processElement(String s, Context context, Collector<DataEvent> collector)  {
        try {
            JSONObject json = JSONObject.parseObject(s);
            JSONObject after = json.getJSONObject("payload").getJSONObject("after") ;
            if (after == null){
                after = json.getJSONObject("payload").getJSONObject("before") ;
            }
            JSONObject source = json.getJSONObject("payload").getJSONObject("source");
            DataEvent dataEvent  = new DataEvent();
            dataEvent.setData(after);
            dataEvent.setConnector(source.getString("connector"));
            dataEvent.setDb(source.getString("db"));
            dataEvent.setTable(source.getString("table"));
            collector.collect(dataEvent);
            //暂时不做任何处理
        }catch (Exception e){
            log.error(e.getMessage()+": "+s);
        }

    }
}
