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
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan yang
 */
public class SendToServiceThread implements Runnable {

    String sendIP = null;
    String topic = null;
    RNode node = null;
    String serviceName = null;
    byte[] sendData = null;
    Rule rule = null;
    SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd:HH");
    Map<String, CloseableHttpClient> topicToHttpclient = null;
    ConcurrentHashMap<String, AtomicLong> ruleToCount = null;
    int count = 0;
    Map<String, Map<String, AtomicLong>> ruleToThreadPoolSize = null;
    AtomicLong ruleToSize = null;
    final int TIMEOUT = 5000;//连接超时时间
    final int SO_TIMEOUT = ((Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_TIMEOUT)) * 1000;//数据传输超时
    org.apache.log4j.Logger logger = null;

    {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(SendToServiceThread.class.getName());
    }

    public SendToServiceThread(byte[] sendData, RNode node, Rule rule, String sendIP, int count) {
        this.sendData = sendData;
        this.node = node;
        this.rule = rule;
        this.sendIP = sendIP;
        this.count = count;
        this.topic = rule.getTopic();
        this.serviceName = rule.getServiceName();
        topicToHttpclient = (Map<String, CloseableHttpClient>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_HTTPCLIENT);
        ruleToCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_COUNT);
        ruleToThreadPoolSize = (Map<String, Map<String, AtomicLong>>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_THREADPOOLSIZE);
        ruleToSize = ruleToThreadPoolSize.get(rule.getTopic()).get(rule.getServiceName());
    }

    @Override
    public void run() {
        sendToType0123();
        ruleToSize.decrementAndGet();
    }

    /**
     *
     * send the message typed 0/1/2/3 to the server
     */
    private void sendToType0123() {
        CloseableHttpClient httpClient = topicToHttpclient.get(rule.getTopic());
        String url = "http://" + sendIP;
        HttpPost httppost = null;
        CloseableHttpResponse response = null;
        int i;
        long st = 0L;
        long et = 0L;
        for (i = 0; i < 3; i++) {
            int flag = 0;
            while (true) {
                httppost = new HttpPost(url);
                httppost.setHeader("cmd", "data");
                RequestConfig requestConfig = RequestConfig.custom()
                        .setSocketTimeout(SO_TIMEOUT)
                        .setConnectTimeout(TIMEOUT)
                        .build();
                httppost.setConfig(requestConfig);
                InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(sendData), -1);
                reqEntity.setContentType("binary/octet-stream");
                reqEntity.setChunked(true);
                httppost.setEntity(reqEntity);
                try {
                    st = System.currentTimeMillis();
                    response = httpClient.execute(httppost);
                    et = System.currentTimeMillis();
                    try {
                        if (response.getStatusLine().getStatusCode() == 200) {
                            ByteArrayOutputStream out = new ByteArrayOutputStream();
                            response.getEntity().writeTo(out);
                            String resonseEn = new String(out.toByteArray());
                            if ("-1".equals(resonseEn.split("[\n]")[0])) {
                                logger.error("send " + count + " messages for " + topic + " " + serviceName + " to the " + url + " failed because the service return a wrong infomation " + resonseEn + " use " + (et - st) + " ms");
                            } else {
                                AtomicLong al = ruleToCount.get(topic + serviceName);
                                al.addAndGet(count);
                                Date date = new Date();
                                String time = dateFormat2.format(date);
                                logger.info(time + " just send " + count + " messages for " + topic + " " + serviceName + " " + node.getName() + " to the " + url + " successfully use " + (et - st) + " ms");
                                flag = 1;
                            }
                        } else {
                            logger.info("send " + count + " messages for " + topic + " " + serviceName + " to the " + url + " failed because " + response.getStatusLine() + " use " + (et - st) + " ms");
                        }
                    } finally {
                        EntityUtils.consume(response.getEntity());
                        response.close();
                        break;
                    }
                } catch (ClientProtocolException ex) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex1) {
                    }
                } catch (IOException ex) {
                    et = System.currentTimeMillis();
                    logger.info("send " + count + " messages for " + topic + " " + serviceName + " to the " + url + " failed use " + (et - st) + " ms because " + ex, ex);
                    break;
                } catch (Exception ex) {
                    et = System.currentTimeMillis();
                    logger.info("send " + count + " messages for " + topic + " " + serviceName + " to the " + url + " failed use " + (et - st) + " ms because " + ex, ex);
                    break;
                }
            }

            if (flag == 1) {
                break;
            }
        }

        if (i >= 3) {
            logger.info("some messages have not been sent!");
            rule.removeNode(node);

            ConcurrentLinkedQueue<Object[]> strandedDataTransmit = (ConcurrentLinkedQueue<Object[]>) RuntimeEnv.getParam(GlobalVariables.STRANDED_DATA_TRANSMIT);
            Object[] o = new Object[2];
            o[0] = rule;
            o[1] = sendData;
            strandedDataTransmit.add(o);

            o = new Object[3];
            o[0] = node;
            o[1] = rule;
            o[2] = sendData;
            ConcurrentLinkedQueue<Object[]> detectNodeList = (ConcurrentLinkedQueue<Object[]>) RuntimeEnv.getParam(GlobalVariables.DETECT_NODELIST);
            detectNodeList.add(o);
        }
    }
}