/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
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
 * @author evan yang
 */
public class DetectNodeThread implements Runnable {

    static org.apache.log4j.Logger logger = null;
    RNode node = null;
    Rule r = null;
    byte[] data = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(DetectNodeThread.class.getName());
    }

    public DetectNodeThread(RNode node, Rule r, byte[] data) {
        this.node = node;
        this.r = r;
        this.data = data;
    }

    public DetectNodeThread() {
    }

    /**
     *
     * detect the node by using the true data until it is connected
     */
    @Override
    public void run() {
        String sendIP = node.getName();
        String url = "http://" + sendIP;
        HttpPost httppost = null;
        HttpClient httpClient = null;
        httpClient = new DefaultHttpClient();
        httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 2000);
        httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 20000);

        while (true) {
            try {
                logger.info("begin to detect the node" + url + " for " + r.getTopic() + " " + r.getServiceName());
                httppost = new HttpPost(url);
                httppost.setHeader("cmd", "detect");
                InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(data), -1);
                reqEntity.setContentType("binary/octet-stream");
                reqEntity.setChunked(true);
                httppost.setEntity(reqEntity);
                HttpResponse response = httpClient.execute(httppost);
                if (response.getStatusLine().getStatusCode() == 200) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    response.getEntity().writeTo(out);
                    String resonseEn = new String(out.toByteArray());
                    if ("-1".equals(resonseEn.split("[\n]")[0])) {
                        logger.info(resonseEn.split("[\n]")[1]);
                        EntityUtils.consume(response.getEntity());
                        httppost.releaseConnection();
                        try {
                            Thread.sleep(5000);
                        } catch (Exception ex1) {
                            logger.error(ex1);
                        }
                        continue;
                    }
                    r.addNode(node);
                    httpClient.getConnectionManager().shutdown();
                    logger.info("connect to the node " + url + " for " + r.getTopic() + " " + r.getServiceName() + " successfully!");
                    break;
                } else {
                    EntityUtils.consume(response.getEntity());
                    httppost.releaseConnection();
                    try {
                        Thread.sleep(5000);
                    } catch (Exception ex1) {
                        logger.error(ex1);
                    }
                    continue;
                }
            } catch (Exception e) {
                logger.error("connect to the node " + url + " for " + r.getTopic() + " " + r.getServiceName() + " failed or timeout !" + e, e);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex1) {
                    logger.error(ex1);
                }
            }
        }
        httpClient.getConnectionManager().shutdown();

        ArrayList<RNode> detectNode = (ArrayList<RNode>) RuntimeEnv.getParam(GlobalVariables.DETECT_NODE);
        
        synchronized (GlobalVariables.SYN_DETECT_NODE) {
            detectNode.remove(node);
        }
    }
}
