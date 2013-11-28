/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class SendControl {

    String url = null;
    String controlType = null;
    String db = null;
    String time = null;
    String part = null;
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(DetectNodeThread.class.getName());
    }

    public SendControl(String url, String controlType, String db, String time, String part) {
        this.url = url;
        this.controlType = controlType;
        this.db = db;
        this.time = time;
        this.part = part;
    }

    public HttpResponse send() {
        if (controlType.equals("start")) {
            logger.info("sending the commander start to the " + url);
            String sendIP = "http://" + url;
            HttpPost httppost = null;
            HttpClient httpClient = null;
            httpClient = new DefaultHttpClient();
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 2000);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 20000);

            HttpResponse response = null;
            for (int i = 0; i < 3; i++) {
                try {
                    httppost = new HttpPost(sendIP);

                    httppost.setHeader("commander", "start");
                    byte[] data = (new String("start|" + db + time + part)).getBytes();
                    InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), -1);
                    reqEntity.setContentType("binary/octet-stream");
                    reqEntity.setChunked(true);
                    httppost.setEntity(reqEntity);
                    System.out.println(httppost);
                    response = httpClient.execute(httppost);
                    EntityUtils.consume(response.getEntity());
                    break;
                } catch (IOException ex) {
                    logger.error(ex, ex);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex1) {
                        logger.error(ex1, ex1);
                    }
                }
            }
            return response;
        } else if (controlType.equals("stop")) {
            logger.info("sending the commander stop to the " + url);
            String sendIP = "http://" + url;
            HttpPost httppost = null;
            HttpClient httpClient = null;
            httpClient = new DefaultHttpClient();
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 2000);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 20000);
            httppost = new HttpPost(sendIP);
            httppost.setHeader("commander", "stop");

            byte[] data = (new String("stop|" + db + time + part)).getBytes();
            InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), -1);
            reqEntity.setContentType("binary/octet-stream");
            reqEntity.setChunked(true);
            httppost.setEntity(reqEntity);
            HttpResponse response = null;
            for (int i = 0; i < 3; i++) {
                try {
                    response = httpClient.execute(httppost);
                    break;
                } catch (IOException ex) {
                    logger.error(ex, ex);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex1) {
                        logger.error(ex1, ex1);
                    }
                }
            }
            return response;
        }
        return null;
    }
}
