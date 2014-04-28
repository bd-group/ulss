/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.sender;

import cn.ac.iie.ulss.match.worker.Matcher;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import org.apache.log4j.Logger;

public class RocketMQProducer {

    public static Logger log = Logger.getLogger(RocketMQProducer.class.getName());
    public static DefaultMQProducer producer = null;

    public static void init(String producerGroupname, String namesrvAddr) {
        log.info("init rocket mq ok for " + producerGroupname);
        RocketMQProducer.producer = RocketMQProducer.getDefaultMQProducer(producerGroupname, namesrvAddr);
    }

    private static DefaultMQProducer getDefaultMQProducer(String producerGroupname, String namesvrAddr) {
        producer = new DefaultMQProducer(producerGroupname);
        producer.setNamesrvAddr(namesvrAddr);
        producer.setInstanceName(producerGroupname + Matcher.ip);
        producer.setClientIP(Matcher.ip);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setCompressMsgBodyOverHowmuch(1024 * 1024 * 4);
        producer.setSendMsgTimeout(3 * 1000);
        producer.setMaxMessageSize(8 * 1024 * 1024);
        producer.setClientCallbackExecutorThreads(50);
        try {
            producer.start();
        } catch (MQClientException ex) {
            log.error(ex, ex);
        }
        return producer;
    }

    public static void sendMessage(String topic, byte[] pData, long count) {
        long bg = System.currentTimeMillis();
        SendResult sendResult = null;
        Message msg = new Message(topic, pData);
        while (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {     //Message msg = new Message("TopicTest1", "TagA", "OrderID001", ("Hello MetaQ").getBytes());
            try {
                sendResult = producer.send(msg);
                if (sendResult == null || sendResult.getSendStatus() == SendStatus.FLUSH_DISK_TIMEOUT) {
                    if (sendResult == null) {
                        log.warn("send message fail,the send result is null,will sleep and retry ...");
                    } else {
                        log.warn("send message fail one time for write disk fail,will sleep and retry,the information is " + producer.getClientIP() + " " + producer.getProducerGroup());
                    }
                    try {
                        Thread.sleep(200);
                    } catch (Exception e) {
                    }
                    continue;
                } else {
                    log.info("send " + count + " records to rocket use " + (System.currentTimeMillis() - bg) + " ms for " + topic + ":" + sendResult);
                    break;
                }
            } catch (Exception ex) {
                log.error(ex + ",the information is:topic --> " + topic + ",will retry,", ex);
                try {
                    Thread.sleep(200);
                } catch (Exception e) {
                }
                continue;
            } finally {
            }
        }
    }

    public void destroyPool() {
        RocketMQProducer.producer.shutdown();
    }
}