package com.bigdata.map;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.model.DataEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 将原始数据转换为 DataEvent
 * @Author: PAN.LU
 * @Date: 2022/8/12 22:35
 */
@Slf4j
public class TransformFunction implements MapFunction<String, DataEvent> {
    @Override
    public DataEvent map(String s) throws Exception {
        try {
            JSONObject json = JSONObject.parseObject(s);
            // 判断 op 为 c 然后提取 after, 否则直接跳过
            if (StringUtils.equals(json.getString("op"),"c")){
                JSONObject after = json.getJSONObject("after");
                // 将 indexName 提取出来,并从原数据中删除
                String indexName = after.getString("indexName");
                after.remove("indexName");
                // todo 这里可以在 after 中加入任意自定义的字段
                DataEvent dataEvent  = new DataEvent();
                dataEvent.setData(after.toJSONString());
                dataEvent.setIndexName(indexName);
                return dataEvent;
            }else {
                log.warn("原始数据中的 op字段不为 c. "+ json);
            }
        }catch (Exception e){
            log.error(e.getMessage()+": "+s);
        }
        return null;
    }
}
