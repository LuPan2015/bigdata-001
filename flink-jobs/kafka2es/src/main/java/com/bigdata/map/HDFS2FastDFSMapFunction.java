package com.bigdata.map;

import com.bigdata.model.DataEvent;
import com.bigdata.util.FileUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lupan on 2022-07-27
 */
public class HDFS2FastDFSMapFunction implements FlatMapFunction<DataEvent,Object> {

    private Map<String,Object> map = new HashMap<>();
    public HDFS2FastDFSMapFunction(String fields) {
       String[] field = fields.split(",");
        for (String f : field) {
            map.put(f.split(".")[0]+"_"+f.split(".")[2],"");
        }
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
    public void flatMap(DataEvent dataEvent, Collector<Object> collector) throws Exception {
        if (dataEvent.getData().containsKey("content")){

        }
        if (dataEvent.getData().containsKey("image")){
            //数据转存
            String hdfsPath = dataEvent.getData().getString("image");
            String goFastDFSPath = FileUtil.dataDump(hdfsPath);
            dataEvent.getData().put("image_path",goFastDFSPath);
            // 图片转文字
            String image_content = "";
            dataEvent.getData().put("image_content",image_content);

        }
        if (dataEvent.getData().containsKey("video")){

        }

        //        String table = dataEvent.getTable();
//        String filed = "";
//        if (!map.containsKey(table+"_"+filed)){
//            return;
//        }
//        // 做数据的上传和下载
//        String url = map.get(table+"_"+filed).toString();
//        // 从 hdfs 下载
//        byte[] inputStream = FileUtil.downloadFileFromHdfs(url);
//        String path = "";
//        String file = "";
//        String goFastDFSPath =FileUtil.uploadFileToGOFastDFS(path,inputStream,file);
//        System.out.println(goFastDFSPath);
    }

}
