/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.avro.generic.GenericRecord;

/**
 *
 * @author work
 */
public class RocketDataPuller implements Runnable {

    public static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(RocketDataPuller.class.getName());
    public String consumerGroup = null;
    public DefaultMQPushConsumer consumer = null;
    public List<String> topics = null;
    public String tag = null;
    public ArrayBlockingQueue<GenericRecord> inbuffer = null;

    public RocketDataPuller(String consumerGroup, List<String> topics, String tag, ArrayBlockingQueue buffer) {
        this.consumerGroup = consumerGroup;
        this.tag = tag;
        this.topics = topics;
        this.inbuffer = buffer;
        this.consumer = new DefaultMQPushConsumer(this.consumerGroup);
    }

    @Override
    public void run() {
        try {
            log.info("wil sleep some time before consume data");
            Thread.sleep(10 * 1000);
        } catch (Exception e) {
            log.error(e, e);
        }
        try {
            consumer.setNamesrvAddr(GlobalParas.rocketNameServer);
            log.info("the name server is " + GlobalParas.rocketNameServer);
            /**
             * 订阅指定topic下所有消息,注意：一个consumer对象可以订阅多个topic
             */
            for (String s : this.topics) {
                consumer.subscribe(s, this.tag);
            }
            consumer.setClientIP(GlobalParas.ip);
            //consumer.setInstanceName(this.consumerGroup + "_consumer_instance");
            consumer.setInstanceName(GlobalParas.hostName + "_consumer_instance");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.setConsumeThreadMin(4);
            consumer.setConsumeThreadMax(GlobalParas.rocketMaxConsumeThread);
            consumer.setConsumeMessageBatchMaxSize(GlobalParas.rocketConsumeMessageBatchMaxSize);
            consumer.setPullThresholdForQueue(GlobalParas.pullThresholdForQueue);
            //consumer.setPullInterval(0);
            //consumer.setPullBatchSize(32);
            consumer.setClientCallbackExecutorThreads(GlobalParas.clientCallbackExecutorThreads);
        } catch (Exception ex) {
            log.error(ex, ex);
        }
        consumer.registerMessageListener(new RocketdataHandler(this.inbuffer));
        try {
            consumer.start();
        } catch (Exception ex) {
            log.error(ex, ex);
        }
        /*
         * * * * fix me
         */
        //Thread.currentThread().setDaemon(true);

        log.info("consumer start ok for " + this.topics + " ");
    }

    public void addTopic(String topic, String tag) {
        try {
            this.consumer.subscribe(topic, tag);
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }

    public void removeTopic(String topic) {
        try {
            this.consumer.unsubscribe(topic);
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }

    public void shutDownAll() {
        try {
            for (String s : this.topics) {
                this.consumer.unsubscribe(s);
            }
            this.consumer.shutdown();
        } catch (Exception ex) {
            log.error(ex, ex);
        }
    }

    public static void main(String[] args) {
        RocketDataPuller puller = new RocketDataPuller("ConsumerGroupName_000", null, "", new ArrayBlockingQueue(1000));
        Thread t = new Thread(puller);
        t.start();
    }
}
