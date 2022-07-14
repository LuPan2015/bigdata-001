package com.bigdata.map;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * @author lupan on 2022-07-12
 */
@Slf4j
public class ElasticSearchFunction extends ProcessFunction<String, Event> {
    @Override
    public void processElement(String s, Context context, Collector<Event> collector) throws Exception {
            JSONObject json = JSONObject.parseObject(s);
            JSONObject after = json.getJSONObject("payload").getJSONObject("after") ;
            if (after == null){
                after = json.getJSONObject("payload").getJSONObject("before") ;
            }
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
            for(String  field : after.keySet()){
                xContentBuilder.field(field,after.getString(field));
            }
            xContentBuilder.endObject();
            JSONObject source = json.getJSONObject("payload").getJSONObject("source");
            Event event = new Event();
            event.setIndexName(source.getString("connector")+"-"+source.getString("db")+"-"+source.getString("table"));
            event.setType("docs");
            event.setData(xContentBuilder.toString());
            //暂时不做任何处理
            collector.collect(event);
    }
}
