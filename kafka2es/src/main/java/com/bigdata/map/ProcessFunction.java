package com.bigdata.map;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.config.Config;
import com.bigdata.model.DataEvent;
import com.bigdata.util.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author lupan on 2022-07-12
 */
@Slf4j
public class ProcessFunction extends org.apache.flink.streaming.api.functions.ProcessFunction<DataEvent, DataEvent> {

    final OutputTag<DataEvent> outputEs = new OutputTag<DataEvent>("es"){};
    final OutputTag<String> outputKafka = new OutputTag<String>("kafka"){};

    private Config config;

    public ProcessFunction(Config config) {
        this.config = config;
    }

    @Override
    public void processElement(DataEvent dataEvent, Context context, Collector<DataEvent> out)  {
        JSONObject data = JSONObject.parseObject(dataEvent.getData());
        if (data.containsKey("content")){
            //调用文本服务
            JSONObject content_param = new JSONObject();
            content_param.put("content",data.getString("content"));
            String contentUrl = config.getContentUrl();
            String result = Util.callPost(contentUrl,content_param.toJSONString());
            String content = result;
            data.put("content",content);
            //数据写 es
            context.output(outputEs,dataEvent);
        }
        if (data.containsKey("image")){
            //数据转存
            String hdfsPath = data.getString("image");
            String goFastDFSPath = Util.dataDump(config.getHdfs(),config.getGofastdfs(),hdfsPath);
            data.put("image_path",goFastDFSPath);
            // 调用 ai 和 ocr 服务
            callAIAndOCR(data,goFastDFSPath);
            //数据写 es
            context.output(outputEs,dataEvent);
        }
        if (data.containsKey("video")){
            // 数据转存
            String hdfsPath = data.getString("video");
            String goFastDFSPath = Util.dataDump(config.getHdfs(),config.getGofastdfs(),hdfsPath);
            // 数据切真
            // 数据调用 ocr 和 ai 服务
            callAIAndOCR(data,goFastDFSPath);

            //将数据写 kafka
            context.output(outputKafka,dataEvent.toString());

        }
    }

    private void callAIAndOCR(JSONObject data, String goFastDFSPath) {
        //请求参数
        JSONObject param = new JSONObject();
        param.put("path",goFastDFSPath);

        // 调用 ai  人脸识别服务，将图片转文字，最终存入 es
        String ai_url = config.getAiUrl();
        String result = Util.callPost(ai_url,param.toJSONString());
        String image_ai_content = result;
        data.put("image_ai_content",image_ai_content);

        //调用 ocr 图片服务，将图片转文字，最终存入 es
        String ocr_url = config.getOrcUrl();
        String ocr_result = Util.callPost(ocr_url,param.toJSONString());
        String image_ocr_content = ocr_result;
        data.put("image_ocr_content",image_ocr_content);
    }

}
