package com.bigdata.util;


//import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * @author lupan on 2022-07-27
 */
//@Slf4j
public class Util {
    /**
     * 将数据从 hdfs 转移到 gofastdfs
     * @param hdfsPath
     * @return
     */
    public static String dataDump(String hdfsPath) {
        byte[] inputStream = Util.downloadFileFromHdfs(hdfsPath);
        String path = "";
        String file = "";
        String goFastDFSPath = Util.uploadFileToGOFastDFS(path,inputStream,file);
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

    /**
     * 发送 post 请求
     * @param url
     * @param param
     * @return
     */
    public static String callPost(String url,String param) {
        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .build();
        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json;charset=utf-8"),param);
        Request request = new Request.Builder()
                .post(requestBody)
                .url(url)
                .build();
        try {
           Response response = okHttpClient.newCall(request).execute();
           if (response.isSuccessful()){
               String result = response.body().toString();
               return result;
           }else {
               System.err.println(url + ", " + param + " 调用失败" );
           }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
}
