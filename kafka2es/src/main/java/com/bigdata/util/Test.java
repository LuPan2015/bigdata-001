package com.bigdata.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * @Author: PAN.LU
 * 测试从 hdfs 下载，再上传到 gofastdfs
 * @Date: 2022/8/10 21:00
 */
public class Test {
//    public static void test1 ()throws Exception{
//        //docker run --network=host --name fastdfs -v /data/fastdfs_data:/data -p 8080:8080 -e GO_FASTDFS_DIR=/data sjqzhang/go-fastdfs
//        String path = "E:\\code\\bigdata\\bigdata-001\\flink-jobs\\README.md";
//        InputStream inputStream = new FileInputStream(new File(path));
//        String p = "http://localhost:8080/group1/upload";
//        String name = "2.csv";
//        Util.uploadFileToGOFastDFS(p,inputStream.readAllBytes(),name);
//    }

    //https://towardsdatascience.com/hdfs-simple-docker-installation-guide-for-data-science-workflow-b3ca764fc94b
//    public static void test2() throws Exception {
//        //String url = "http://localhost:9870/webhdfs/v1/aaa/1.txt?op=OPEN&";
//        String url = "http://localhost:9864/webhdfs/v1/aaa/1.txt?op=OPEN&namenoderpcaddress=namenode:9000&offset=0";
//        byte[] inputStream = Util.downloadFileFromHdfs(url);
//        System.out.println(new String(inputStream));
//        String p = "http://localhost:8080/group1/upload";
//        String name = "3.csv";
//        System.out.println(Util.uploadFileToGOFastDFS(p,inputStream,name));
//    }

    public static void main(String[] args)throws Exception{
        //test2();
        String a = "hdfs://127.0.0.1:8020/abc/test.txt";
        System.out.println(a.split("8020")[1]);
    }
}
