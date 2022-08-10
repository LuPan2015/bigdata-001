package com.bigdata.map;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.config.Config;
import com.bigdata.model.DataEvent;
import com.bigdata.util.Util;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author lupan on 2022-07-27
 */
public class HDFS2FastDFSMapFunction implements FlatMapFunction<DataEvent,DataEvent> {

 //   private Map<String,Object> map = new HashMap<>();
    private Config config;
    public HDFS2FastDFSMapFunction(Config config) {
        this.config = config;
//       String[] field = fields.split(",");
//        for (String f : field) {
//            map.put(f.split(".")[0]+"_"+f.split(".")[2],"");
//        }
    }

    /**
     * 1. 如果字段名为 content,调用文本服务将结果写到 es. 放到一个字段
     * 2. 如果字段名为 image,调用hdfs download, upload gofast 的 url, 调用人脸识别
     * 3. 如果字段名为 video,调用hdfs download, upload gofast 的 url, 切开, 调用人脸识别，返回结果
     * @param dataEvent
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap(DataEvent dataEvent, Collector<DataEvent> collector) throws Exception {
        if (dataEvent.getData().containsKey("content")){
            //调用文本服务
            JSONObject content_param = new JSONObject();
            content_param.put("content",dataEvent.getData().getString("content"));
            String contentUrl = config.getContentUrl();
            String result = Util.callPost(contentUrl,content_param.toJSONString());
            String content = result;
            dataEvent.getData().put("content",content);
        }
        if (dataEvent.getData().containsKey("image")){
            //数据转存
            String hdfsPath = dataEvent.getData().getString("image");
            String goFastDFSPath = Util.dataDump(config.getHdfs(),config.getGofastdfs(),hdfsPath);
            dataEvent.getData().put("image_path",goFastDFSPath);
            // 调用 ai 和 ocr 服务
            callAIAndOCR(dataEvent,goFastDFSPath);
        }
        if (dataEvent.getData().containsKey("video")){
            // 数据转存
            String hdfsPath = dataEvent.getData().getString("video");
            String goFastDFSPath = Util.dataDump(config.getHdfs(),config.getGofastdfs(),hdfsPath);
            // 数据切真
            // 数据调用 ocr 和 ai 服务
            callAIAndOCR(dataEvent,goFastDFSPath);
        }
        //最终将数据流到下一个算子
        collector.collect(dataEvent);
    }

    private void callAIAndOCR(DataEvent dataEvent, String goFastDFSPath) {
        //请求参数
        JSONObject param = new JSONObject();
        param.put("path",goFastDFSPath);

        // 调用 ai  人脸识别服务，将图片转文字，最终存入 es
        String ai_url = config.getAiUrl();
        String result = Util.callPost(ai_url,param.toJSONString());
        String image_ai_content = result;
        dataEvent.getData().put("image_ai_content",image_ai_content);

        //调用 ocr 图片服务，将图片转文字，最终存入 es
        String ocr_url = config.getOrcUrl();
        String ocr_result = Util.callPost(ocr_url,param.toJSONString());
        String image_ocr_content = ocr_result;
        dataEvent.getData().put("image_ocr_content",image_ocr_content);
    }

}
