/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.datastatics;

import cn.ac.iie.ulss.indexer.worker.Indexer;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

public class MQProducerPool {

    public static Logger log = Logger.getLogger(MQProducerPool.class.getName());
    private static MessageSessionFactory sessionFactory = null;
    private String name;
    private int poolSize;
    MessageProducer[] producer = null;
    private AtomicInteger idx = new AtomicInteger(0);

    private MQProducerPool(String pMQName, int pPoolSize) {
        name = pMQName;
        poolSize = pPoolSize;
        producer = new MessageProducer[pPoolSize];
        for (int i = 0; i < poolSize; i++) {
            producer[i] = sessionFactory.createProducer();
            producer[i].publish(name);
        }
    }

    public static MQProducerPool getMQProducerPool(String pMQName, int pPoolSize) {
        if (sessionFactory == null) {
            try {
                MetaClientConfig metaqClientConfig = new MetaClientConfig();
                ZKConfig zkConfig = new ZkUtils.ZKConfig();
                zkConfig.zkConnect = Indexer.zkUrls;
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

    public void sendMessage(byte[] pData) {
        Message message = new Message(name, pData);
        SendResult sendResult = null;
        while (sendResult == null || !sendResult.isSuccess()) {
            try {
                sendResult = producer[(idx.addAndGet(1)) % poolSize].sendMessage(message, 100, TimeUnit.SECONDS);
                idx.compareAndSet(1000000000, 0);
                if (sendResult == null || !sendResult.isSuccess()) {
                    log.warn("send message fail one time,will sleep and retry ...");
                    try {
                        Thread.sleep(5000);
                    } catch (Exception e) {
                    }
                    continue;
                } else {
                    return;
                }
            } catch (Exception ex) {
                log.error(ex, ex);
                idx.compareAndSet(1000000000, 0);
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                }
                continue;
            }
        }
    }
}