/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class DataAccepter {

    int handlertype = 0;
    int blockemittersize = 4;
    int mqfetchrunnersize = 12;
    Map<String, ArrayList<Rule>> topicToRules = null;
    Map<String, BlockEmitter[]> topicToEmitters = null;
    Map<String, ConcurrentLinkedQueue[]> topicToDataPool = null;
    AtomicLong number = new AtomicLong(0);
    String mqnameserver = null;
    DefaultMQPushConsumer consumer = null;
    String nodename = null;
    String rmqgroup = null;
    private Logger logger;

    {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(DataAccepter.class.getName());
    }

    public void init() {
        handlertype = (Integer) RuntimeEnv.getParam(RuntimeEnv.HDNDLER_TYPE);
        blockemittersize = (Integer) RuntimeEnv.getParam(RuntimeEnv.DATA_POOL_COUNT);
        topicToRules = (Map<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES);
        topicToEmitters = new HashMap<String, BlockEmitter[]>();
        topicToDataPool = (Map<String, ConcurrentLinkedQueue[]>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_DATAPOOL);
        mqnameserver = (String) RuntimeEnv.getParam(RuntimeEnv.MQ_NAME_SERVER);
        mqfetchrunnersize = (Integer) RuntimeEnv.getParam(RuntimeEnv.MQ_FETCH_RUNNER_SIZE);
        nodename = (String) RuntimeEnv.getParam(RuntimeEnv.NODENAME);
        rmqgroup = (String) RuntimeEnv.getParam(RuntimeEnv.RMQGROUP);
    }

    public void pullDataFromQ() {
        logger.info("begin to pull the data from rmq!");
        consumer = new DefaultMQPushConsumer("rd_" + rmqgroup);
        consumer.setNamesrvAddr(mqnameserver);
        consumer.setInstanceName("rd_consumer_" + nodename);
        consumer.setConsumeThreadMin(mqfetchrunnersize);
        consumer.setConsumeThreadMax(mqfetchrunnersize);
        consumer.setConsumeConcurrentlyMaxSpan(1024);
        consumer.setPullThresholdForQueue(16);
        consumer.setPullBatchSize(16);
        consumer.setConsumeMessageBatchMaxSize(8);
        consumer.setClientCallbackExecutorThreads(32);

        for (String topic : topicToRules.keySet()) {
            try {
                consumer.subscribe(topic, "*");
                logger.info("consumer the topic " + topic);
            } catch (MQClientException ex) {
                logger.error(ex, ex);
                return;
            }
            ConcurrentLinkedQueue[] dataPool = topicToDataPool.get(topic);
            BlockEmitter[] emitters = null;
            AtomicLong count = new AtomicLong(1);
            AtomicLong version = new AtomicLong(1);
            AtomicLong time = new AtomicLong(System.currentTimeMillis());
            emitters = new BlockEmitter[blockemittersize];
            for (int i = 0; i < blockemittersize; i++) {
                BlockEmitter emitter = new BlockEmitter(dataPool, topic, i, handlertype, count, version, time);
                emitter.init();
                emitters[i] = emitter;
            }
            topicToEmitters.put(topic, emitters);
            if (handlertype == 0 || handlertype == 1) {
                DetectAcceptTimeout dat = new DetectAcceptTimeout(emitters, topic, dataPool, count, version, time);
                Thread tdat = new Thread(dat);
                tdat.setName("DetectAcceptTimeout-" + topic);
                tdat.start();
            }
        }

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    BlockEmitter[] emitter = topicToEmitters.get(msg.getTopic());
                    if (emitter == null) {
                        logger.error("get a error message from the rmq " + msg.getTopic());
                        continue;
                    }
                    Random r = new Random();
                    int v = r.nextInt(emitter.length);
                    emitter[v].emit(msg.getBody(), "data", System.currentTimeMillis());
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        try {
            consumer.start();
        } catch (MQClientException ex) {
            logger.error(ex, ex);
        }
    }

    public void shutdown() {
        consumer.shutdown();;
    }
}
