/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.datadispatch.handler.datadispatch;

import cn.ac.iie.ulss.datadispatch.commons.RuntimeEnv;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.utils.ZkUtils;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author ulss
 */
public class MQProducerPool {
    //metaq configurations

    private static MessageSessionFactory sessionFactory = null;
    private String name;
    private int poolSize;
    MessageProducer[] producer = null;
    private int idx = 0;
    private BlockingQueue producerSet = null;

    private MQProducerPool(String pMQName, int pPoolSize) {

        name = pMQName;        
        poolSize = pPoolSize;
        producer = new MessageProducer[pPoolSize];
        for(int i=0;i<poolSize;i++){
            producer[i] = sessionFactory.createProducer();
            producer[i].publish(name);
        }
//        producerSet = new LinkedBlockingQueue();
//        for (int i = 0; i < poolSize; i++) {
//
//            MessageProducer producer = sessionFactory.createProducer(); //maybe failed
//            producer.publish(name);
//            try {
//                producerSet.put(producer);
//            } catch (Exception ex) {
//            }
//        }
    }

    public static MQProducerPool getMQProducerPool(String pMQName, int pPoolSize) {
        if (sessionFactory == null) {
            try {
                MetaClientConfig metaqClientConfig = new MetaClientConfig();
                ZKConfig zkConfig = new ZkUtils.ZKConfig();
                zkConfig.zkConnect = (String) RuntimeEnv.getParam(RuntimeEnv.ZK_CLUSTER);
                metaqClientConfig.setZkConfig(zkConfig);
                sessionFactory = new MetaMessageSessionFactory(metaqClientConfig);
            } catch (Exception ex) {
                sessionFactory = null;
                return null;
            }
        }
        return new MQProducerPool(pMQName, pPoolSize);
    }

    public void sendMessage(byte[] pData) throws Exception{
        Message message = new Message(name, pData);
//        MessageProducer producer = null;
        try {
//            producer = (MessageProducer) producerSet.take();
//            long startTime = System.nanoTime();
//            System.out.println(startTime);
            SendResult sendResult = producer[(idx++)%poolSize].sendMessage(message, 100, TimeUnit.SECONDS);

            if (!sendResult.isSuccess()) {
                System.err.println("Send message failed,error message:" + sendResult.getErrorMessage());
            } else {
//                long endTime = System.nanoTime();
//                System.out.println(endTime);
//                System.out.println("ok:" + (endTime - startTime) / (1000 * 1000));
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
//                producerSet.put(producer);
            } catch (Exception ex) {
            }
        }
    }
}
