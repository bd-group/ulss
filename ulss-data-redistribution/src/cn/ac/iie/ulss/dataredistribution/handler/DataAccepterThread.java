package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils;
import org.apache.log4j.PropertyConfigurator;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import org.apache.log4j.Logger;

/**
 *
 * @author evan yang
 */
public class DataAccepterThread implements Runnable {

    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String zkUrl = null;
    String topic = null;
    ConcurrentLinkedQueue dataPool = null;
    Map<String, MessageConsumer> topicToConsumer = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(DataAccepterThread.class.getName());
    }

    public DataAccepterThread(String zkUrl, String topic, ConcurrentLinkedQueue dataPool) {
        this.zkUrl = zkUrl;
        this.topic = topic;
        this.dataPool = dataPool;
    }

    @Override
    public void run() {
        topicToConsumer = (Map<String, MessageConsumer>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_CONSUMER);
        pullDataFromQ();
    }

    /**
     *
     * 根据表的名字获得对应的消息队列的名字，然后根据消息队列的分区从中拉取数据,然后放入bufferPool中
     */
    public void pullDataFromQ() {
        BlockEmitter emitter = new BlockEmitter(dataPool, topic);
        emitter.init();
        DetectAcceptTimeout dat = new DetectAcceptTimeout(emitter, topic, dataPool);
        Thread tdat = new Thread(dat);
        tdat.setName("DetectAcceptTimeout-" + topic);
        tdat.start();

        System.setProperty("notify.remoting.max_read_buffer_size", "10485760");
        MetaClientConfig metaClientConfig = new MetaClientConfig();
        final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
        zkConfig.zkConnect = this.zkUrl;
        metaClientConfig.setZkConfig(zkConfig);
        final MessageSessionFactory sessionFactory;
        try {
            sessionFactory = new MetaMessageSessionFactory(metaClientConfig);

            ConsumerConfig cc = new ConsumerConfig("rd_" + topic + "_consumer");

            final MessageConsumer consumer = sessionFactory.createConsumer(cc);

            logger.info("init the consumer ok,begin receive data from the topic " + this.topic);

            consumer.subscribe(topic, 10 * 1024 * 1024, new MessageListenerImpl(emitter));

            consumer.completeSubscribe();

            topicToConsumer.put(topic, consumer);
        } catch (MetaClientException ex) {
            logger.error(ex, ex);
        }
    }

    /**
     *
     * the messageListener use control the flow
     */
    private class MessageListenerImpl implements MessageListener {

        BlockEmitter be = null;

        public MessageListenerImpl(BlockEmitter be) {
            this.be = be;
        }

        @Override
        public void recieveMessages(Message msg) {
            if (msg != null) {
//                while (!bufferPool.offer(msg)) {
//                    logger.debug("the  bufferPool is full,and will sleep ...");
//                    try {
//                        Thread.sleep(10);
//                    } catch (Exception ex) {
//                        //do nothing
//                    }
//                }
                be.emit(msg, "data", System.currentTimeMillis());
            }
        }

        @Override
        public Executor getExecutor() {
            return null;
        }
    }
}
