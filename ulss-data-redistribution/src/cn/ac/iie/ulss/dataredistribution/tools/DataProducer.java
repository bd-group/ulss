package cn.ac.iie.ulss.dataredistribution.tools;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class DataProducer {

    DefaultMQProducer producer = null;
    String mqnameserver = null;
    String rmqgroup = null;
    long st = 0L;
    long et = 0L;
    ConcurrentHashMap<String, AtomicLong> ruleToCount = null;
    SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd:HH");
    Map<String, Map<String, AtomicLong>> ruleToThreadPoolSize = null;
    String nodename = (String) RuntimeEnv.getParam(RuntimeEnv.NODENAME);
    int SO_TIMEOUT = 3000;
    org.apache.log4j.Logger logger = null;

    {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(DataProducer.class.getName());
    }

    public void init() {
        rmqgroup = (String) RuntimeEnv.getParam(RuntimeEnv.RMQGROUP);
        mqnameserver = (String) RuntimeEnv.getParam(RuntimeEnv.MQ_NAME_SERVER);
        SO_TIMEOUT = ((Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_TIMEOUT)) * 1000;
        producer = new DefaultMQProducer("rd_" + rmqgroup);
        producer.setNamesrvAddr(mqnameserver);
        producer.setInstanceName("rd_producer_" + nodename);
        producer.setCompressMsgBodyOverHowmuch(4096);
        producer.setMaxMessageSize(8388608);
        producer.setClientCallbackExecutorThreads(32);
        producer.setSendMsgTimeout(SO_TIMEOUT);
        producer.setRetryTimesWhenSendFailed(3);

        try {
            producer.start();
        } catch (MQClientException ex) {
            logger.error(ex, ex);
        }
        ruleToCount = (ConcurrentHashMap<String, AtomicLong>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_COUNT);
        ruleToThreadPoolSize = (Map<String, Map<String, AtomicLong>>) RuntimeEnv.getParam(GlobalVariables.RULE_TO_THREADPOOLSIZE);
    }

    public void send(String topic, String serviceName, String sendIP, int count, String keyinterval, byte[] sendData) {

        Message msg = new Message(sendIP + "_" + topic, sendData);// body
        SendResult sendResult = null;
        while ((sendResult == null)) {
            try {
                st = System.currentTimeMillis();
                sendResult = producer.send(msg);
                if (sendResult == null) {
                    et = System.currentTimeMillis();
                    logger.warn("send " + count + " messages for " + topic + " " + serviceName + " " + keyinterval + " to the  " + sendIP + " failed use " + (et - st) + " ms ,will sleep and retry ...");
                } else {
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                }
            } catch (Exception ex) {
                et = System.currentTimeMillis();
                logger.error("send " + count + " messages for " + topic + " " + serviceName + " " + keyinterval + " to the  " + sendIP + " failed use " + (et - st) + " ms because " + ex, ex);
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                }
            } finally {
            }
        }
        et = System.currentTimeMillis();
        AtomicLong al = ruleToCount.get(topic + serviceName);
        al.addAndGet(count);
        Date date = new Date();
        String time = dateFormat2.format(date);
        logger.info(time + " just send " + count + " messages for " + topic + " " + serviceName + " " + keyinterval + " to the " + sendIP + "_" + topic + " " + sendResult + " use " + (et - st) + " ms");
        AtomicLong ruleToSize = ruleToThreadPoolSize.get(topic).get(serviceName);
        ruleToSize.decrementAndGet();
    }

    public void shutdown() {
        producer.shutdown();
    }
}
