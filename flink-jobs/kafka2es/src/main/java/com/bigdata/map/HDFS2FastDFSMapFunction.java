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

    @Override
    public void flatMap(DataEvent dataEvent, Collector<Object> collector) throws Exception {
        String table = dataEvent.getTable();
        String filed = "";
        if (!map.containsKey(table+"_"+filed)){
            return;
        }
        // 做数据的上传和下载
        String url = map.get(table+"_"+filed).toString();
        // 从 hdfs 下载
        byte[] inputStream = FileUtil.downloadFileFromHdfs(url);
        String path = "";
        String file = "";
        String goFastDFSPath =FileUtil.uploadFileToGOFastDFS(path,inputStream,file);
        System.out.println(goFastDFSPath);
    }

}
