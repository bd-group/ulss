/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.ByteArrayInputStream;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

/**
 *
 * @author evan yang
 */
public class send {
    public static void run() {
        String sendIP = "127.0.0.1:8187/datatransmit/";//node.getName();
        String url = "http://" + sendIP;
        HttpPost httppost = null;
        HttpClient httpClient = null;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i <800000; i++) {
            sb.append("a");
        }
        byte[] data = sb.toString().getBytes();
        while (true) {
            try {
                httpClient = new DefaultHttpClient();
                //httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 2000);
                HttpParams params = httpClient.getParams();
                HttpConnectionParams.setSoTimeout(params, 2*1000);//设定连接等待时间
                HttpConnectionParams.setConnectionTimeout(params, 2*1000);//设定超时时间
                
                httppost = new HttpPost(url);
                InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), -1);
                reqEntity.setContentType("binary/octet-stream");
                reqEntity.setChunked(true);
                httppost.setEntity(reqEntity);
                System.out.println("begin to detect the node" + url);
                HttpResponse response = httpClient.execute(httppost);
                //r.addNode(node);
                httppost.releaseConnection();
                httpClient.getConnectionManager().shutdown();
                System.out.println("connect to the node " + url + " successfully!");
                break;
            } catch (Exception e) {
                httppost.releaseConnection();
                httpClient.getConnectionManager().shutdown();
                System.out.println("connect to the node " + url + " failed or timeout !" + e);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ex1) {
                    System.out.println(ex1);
                }
            }
        }
    }

    public static void main(String[] args) {
        run();
    }
}
