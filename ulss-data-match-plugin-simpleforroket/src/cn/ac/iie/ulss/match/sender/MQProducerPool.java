/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.sender;

import cn.ac.iie.ulss.match.worker.Matcher;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class MQProducerPool {

    public static Logger log = Logger.getLogger(MQProducerPool.class.getName());
    private static MessageSessionFactory sessionFactory = null;
    private String name;
    private int poolSize;
    private LinkedBlockingQueue<MessageProducer> producerPool = null;

    public static MQProducerPool getMQProducerPool(String pMQName, int pPoolSize) {
        if (sessionFactory == null) {
            try {
                MetaClientConfig metaqClientConfig = new MetaClientConfig();
                ZKConfig zkConfig = new ZkUtils.ZKConfig();
                zkConfig.zkConnect = Matcher.zkUrl;
                metaqClientConfig.setZkConfig(zkConfig);
                sessionFactory = new MetaMessageSessionFactory(metaqClientConfig);
            } catch (Exception ex) {
                log.error(ex, ex);
                sessionFactory = null;
                return null;
            }
        }
        return new MQProducerPool(pMQName, pPoolSize);
    }

    private MQProducerPool(String pMQName, int pPoolSize) {
        this.name = pMQName;
        this.poolSize = pPoolSize;
        this.producerPool = new LinkedBlockingQueue<MessageProducer>();
        for (int i = 0; i < poolSize; i++) {
            MessageProducer producer = sessionFactory.createProducer();
            producer.publish(this.name);
            try {
                this.producerPool.put(producer);
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }
    }

    public void sendMessage(byte[] pData) {
        long bg = System.currentTimeMillis();
        Message message = new Message(name, pData);
        SendResult sendResult = null;
        MessageProducer producer = null;

        while (sendResult == null || !sendResult.isSuccess()) {
            try {
                producer = producerPool.take();
                sendResult = producer.sendMessage(message, 45, TimeUnit.SECONDS);
                if (sendResult == null || !sendResult.isSuccess()) {
                    if (sendResult == null) {
                        log.warn("send message fail one time,will sleep and retry ...");
                    } else {
                        log.warn("send message fail one time,will sleep and retry,the partition is " + sendResult.getPartition());
                    }
                    try {
                        Thread.sleep(5000);
                    } catch (Exception e) {
                    }
                    continue;
                } else {
                    log.info("send to metaq use " + (System.currentTimeMillis() - bg) + " ms for " + this.name);
                    return;
                }
            } catch (Exception ex) {
                log.error(ex + ",the information is:topic--> " + this.name, ex);
                try {
                    producer.shutdown();
                } catch (MetaClientException ex1) {
                    log.error(ex1, ex1);
                }
                producer = null;
                producer = sessionFactory.createProducer();
                producer.publish(this.name);
            } finally {
                try {
                    if (producer != null) {
                        this.producerPool.put(producer);
                    } else {
                        log.error("the producer is null,why ???");
                    }
                } catch (Exception ex) {
                    log.error(ex, ex);
                }
            }
        }
    }

    public void destroyPool() {
        for (MessageProducer mp : producerPool) {
            try {
                mp.shutdown();
            } catch (MetaClientException ex) {
                log.error(ex, ex);
            }
        }
        try {
            sessionFactory.shutdown();
        } catch (MetaClientException ex) {
            log.error(ex, ex);
        }
    }
}