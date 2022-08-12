package com.bigdata.util;


import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author lupan on 2022-07-27
 */
public class Util {
    /**
     * 将数据从 hdfs 转移到 gofastdfs
     * @param hdfsPath
     * @return
     */
    public static String dataDump(String hdfsUrl,String gofastUrl,String hdfsPath) {
        /* todo 假设 hdfs 的路径格式为: hdfs://127.0.0.1:8020/abc/test.txt
         则需要将 /abc/test.txt 提取出来，这里先简单的以 8020 这个端口来切分。
         实际情况待看到样例数据进行微调
         */
        String hp = hdfsPath.split("8020")[1];
        byte[] inputStream = Util.downloadFileFromHdfs(hdfsUrl+hp);
        String file = hdfsPath.split("/")[hdfsPath.split("/").length-1];
        String goFastDFSPath = Util.uploadFileToGOFastDFS(gofastUrl,inputStream,file);
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
                .connectTimeout(2, TimeUnit.SECONDS)
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
