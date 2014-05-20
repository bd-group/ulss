/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import java.io.IOException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class SendCreateFileCMD {

    String sendIP = null;
    Long f_id = null;
    String road = null;
    static org.apache.log4j.Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(DetectNodeThread.class.getName());
    }

    SendCreateFileCMD(String sendIP, Long f_id, String road) {
        this.sendIP = sendIP;
        this.f_id = f_id;
        this.road = road;
    }

    public CloseableHttpResponse send() {

        String url = "http://" + sendIP;
        logger.info("sending the create file commander to the " + url);

        HttpPost httppost = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        try {
            httppost = new HttpPost(url);
            httppost.setHeader("cmd", "createfile|" + Long.toString(f_id) + "|" + road);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(5000)
                    .setConnectTimeout(2000)
                    .build();
            httppost.setConfig(requestConfig);
            response = httpClient.execute(httppost);
            return response;
        } catch (IOException ex) {
            logger.error(ex, ex);
            try {
                Thread.sleep(2000);
            } catch (Exception ex1) {
            }
            return null;
        } catch (Exception ex) {
            logger.error(ex, ex);
            try {
                Thread.sleep(2000);
            } catch (Exception ex1) {
            }
            return null;
        }
    }
}