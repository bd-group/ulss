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
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class GetErrorFile {
    
    String mqnameserver = null;
    DefaultMQPushConsumer consumer = null;
    String nodename = null;
    Map<String, ArrayList<Rule>> topicToRules = null;
    Set<String> topics = null;
    private Logger logger;
    
    {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(GetErrorFile.class.getName());
    }
    
    public void init() {
        mqnameserver = (String) RuntimeEnv.getParam(RuntimeEnv.MQ_NAME_SERVER);
        nodename = (String) RuntimeEnv.getParam(RuntimeEnv.NODENAME);
        topicToRules = (Map<String, ArrayList<Rule>>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_RULES);
        topics = topicToRules.keySet();
    }
    
    public void pullDataFromQ() {
        String consumerTimestamp = UtilAll.timeMillisToHumanString(System.currentTimeMillis() - (1000 * 60 * 5));
        logger.info("begin to pull the data from rmq!");
        consumer = new DefaultMQPushConsumer("rdef_" + nodename);
        consumer.setNamesrvAddr(mqnameserver);
        consumer.setInstanceName("rd_consumer_" + nodename);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setConsumeConcurrentlyMaxSpan(100);
        consumer.setPullThresholdForQueue(50);
        consumer.setPullBatchSize(50);
        consumer.setConsumeMessageBatchMaxSize(50);
        consumer.setConsumeTimestamp(consumerTimestamp);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);    
        
        try {
            consumer.subscribe("error_file_mq", "*");
        } catch (MQClientException ex) {
            logger.error(ex, ex);
        }
        
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    String m = new String(msg.getBody());
                    if (msg.getTopic().equals("error_file_mq")) {
                        String[] s = m.split("\\|");
                        if (s.length > 1 && topics.contains(s[1])) {
                            try {
                                long fid = Long.parseLong(s[0]);
                                CheckFile.check(fid);
                            } catch (Exception e) {
                                logger.error("the message " + m + " from error_file_mq is wrong");
                            }
                        } else {
                            logger.debug("the message " + m + " from error_file_mq is wrong");
                        }
                    } else {
                        logger.error("get a error message from the error_file_mq " + msg.getTopic());
                    }
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