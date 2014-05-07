/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import org.apache.log4j.Logger;

public class RocketMQProducer {

    public static Logger log = Logger.getLogger(RocketMQProducer.class.getName());
    private static DefaultMQProducer producer = null;

    public static void init(String producerGroupname, String namesrvAddr) {
        log.info("init rocket mq  producer ok for " + producerGroupname);
        RocketMQProducer.producer = RocketMQProducer.getDefaultMQProducer(producerGroupname, namesrvAddr);
    }

    private static DefaultMQProducer getDefaultMQProducer(String producerGroupname, String namesvrAddr) {
        producer = new DefaultMQProducer(producerGroupname);
        producer.setNamesrvAddr(namesvrAddr);

        producer.setInstanceName(producerGroupname + "_producer_instance");
        producer.setClientIP(GlobalParas.ip);
        producer.setClientCallbackExecutorThreads(GlobalParas.clientCallbackExecutorThreads);
        producer.setCompressMsgBodyOverHowmuch(1024 * GlobalParas.compressMsgBodyOverHowmuchInKB);
        producer.setMaxMessageSize(GlobalParas.rocketConsumesetMaxMessageSizeMB * 1024 * 1024);

        try {
            producer.start();
        } catch (MQClientException ex) {
            log.error(ex, ex);
        }
        return producer;
    }

    public static void sendMessage(String topic, byte[] pData) {
        long bg = System.currentTimeMillis();
        SendResult sendResult = null;
        Message msg = new Message(topic, pData);
        while (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
            try {
                sendResult = producer.send(msg);
                if (sendResult == null || sendResult.getSendStatus() == SendStatus.FLUSH_DISK_TIMEOUT) {
                    if (sendResult == null) {
                        log.warn("send message fail one time,will sleep and retry ...");
                    } else {
                        log.warn("send message fail one time,will sleep and retry,the information is " + producer.getClientIP() + " " + producer.getProducerGroup());
                    }
                    try {
                        Thread.sleep(200);
                    } catch (Exception e) {
                    }
                    continue;
                } else {
                    log.info("send to metaq use " + (System.currentTimeMillis() - bg) + " ms for " + topic);
                    return;
                }
            } catch (Exception ex) {
                log.error(ex + ",the information is:topic--> " + topic, ex);
            } finally {
            }
        }
    }

    public static void shutProducer() {
        RocketMQProducer.producer.shutdown();
    }
}
