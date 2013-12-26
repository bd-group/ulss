/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import java.io.IOException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
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

    public HttpResponse send() {

        String url = "http://" + sendIP;
        logger.info("sending the create file commander to the " + url);

        HttpPost httppost = null;
        HttpClient httpClient = null;
        httpClient = new DefaultHttpClient();
        httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 10000);
        httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 20000);
        HttpResponse response = null;
        for (int i = 0; i < 3; i++) {
            try {
                httppost = new HttpPost(url);
                httppost.setHeader("cmd", "createfile|" + Long.toString(f_id) + "|" + road);
                response = httpClient.execute(httppost);
                break;
            } catch (IOException ex) {
                logger.error(ex, ex);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex1) {
                    logger.error(ex1, ex1);
                }
            }
        }
        return response;
    }
}