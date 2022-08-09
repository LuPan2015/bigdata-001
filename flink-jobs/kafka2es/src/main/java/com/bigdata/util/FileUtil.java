package com.bigdata.util;


//import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.flink.runtime.rest.messages.JobAccumulatorsInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author lupan on 2022-07-27
 */
//@Slf4j
public class FileUtil {
    /**
     * 将数据从 hdfs 转移到 gofastdfs
     * @param hdfsPath
     * @return
     */
    public static String dataDump(String hdfsPath) {
        byte[] inputStream = FileUtil.downloadFileFromHdfs(hdfsPath);
        String path = "";
        String file = "";
        String goFastDFSPath = FileUtil.uploadFileToGOFastDFS(path,inputStream,file);
        return goFastDFSPath;
    }

    static OkHttpClient client = new OkHttpClient();
    public static byte[] downloadFileFromHdfs (String url){
        //http://$IP_WEBHDFS:$PORT_WEBHDFS/webhdfs/v1/distant/path/my_distant_file?user.name=my_user&op=OPEN
        Request request = new Request.Builder().url(url).build();
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()){
                System.out.println("文件下载成功." + url);
                return  response.body().bytes();
            }else {
                System.out.println("文件下载失败." + url);
                return null;
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
            return null;
        }
    }

    public static String uploadFileToGOFastDFS(String path,byte[] stream,String filename){
        try {
            MultipartBody multipartBody = new MultipartBody.Builder().
                    setType(MultipartBody.FORM)
                    .addFormDataPart("file", filename,
                            RequestBody.create(MediaType.parse("multipart/form-data;charset=utf-8"),
                                    stream))
                    //.addFormDataPart("output", "json")
                    .build();
            Request request = new Request.Builder()
                    .url(path)
                    .post(multipartBody)
                    .build();
            Response response = client.newCall(request).execute();
            if (response.isSuccessful()){
                String p = response.body().string();
                System.out.println("文件上传成功." + p);
                return p;
            }else {
                System.out.println("文件上传失败." + filename);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void test1 ()throws Exception{
        //docker run --network=host --name fastdfs -v /data/fastdfs_data:/data -p 8080:8080 -e GO_FASTDFS_DIR=/data sjqzhang/go-fastdfs
        String path = "E:\\code\\bigdata\\bigdata-001\\flink-jobs\\README.md";
        InputStream inputStream = new FileInputStream(new File(path));
        String p = "http://localhost:8080/group1/upload";
        String name = "2.csv";
        uploadFileToGOFastDFS(p,inputStream.readAllBytes(),name);
    }

    //https://towardsdatascience.com/hdfs-simple-docker-installation-guide-for-data-science-workflow-b3ca764fc94b
    public static void test2() throws Exception {
        //String url = "http://localhost:9870/webhdfs/v1/aaa/1.txt?op=OPEN&";
        String url = "http://localhost:9864/webhdfs/v1/aaa/1.txt?op=OPEN&namenoderpcaddress=namenode:9000&offset=0";
        byte[] inputStream = downloadFileFromHdfs(url);
        System.out.println(new String(inputStream));
        String p = "http://localhost:8080/group1/upload";
        String name = "3.csv";
        System.out.println(uploadFileToGOFastDFS(p,inputStream,name));
    }

    public static void main(String[] args)throws Exception{
        test2();
    }

}
