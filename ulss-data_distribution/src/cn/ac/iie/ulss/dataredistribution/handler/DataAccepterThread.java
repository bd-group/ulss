package cn.ac.iie.ulss.dataredistribution.handler;

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
import java.util.concurrent.ArrayBlockingQueue;
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
    ArrayBlockingQueue bufferPool = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(DataAccepterThread.class.getName());
    }

    public DataAccepterThread(String zkUrl, String topic, ArrayBlockingQueue bufferPool) {
        this.zkUrl = zkUrl;
        this.topic = topic;
        this.bufferPool = bufferPool;
    }

    @Override
    public void run() {
        pullDataFromQ();
    }

    //根据表的名字获得对应的消息队列的名字，然后根据消息队列的分区从中拉取数据,然后放入bufferPool中
    public void pullDataFromQ() {
        MetaClientConfig metaClientConfig = new MetaClientConfig();
        final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
        zkConfig.zkConnect = this.zkUrl;
        metaClientConfig.setZkConfig(zkConfig);
        final MessageSessionFactory sessionFactory;
        try {
            sessionFactory = new MetaMessageSessionFactory(metaClientConfig);

            ConsumerConfig cc = new ConsumerConfig("rd" + topic + "_consumer");

            final MessageConsumer consumer = sessionFactory.createConsumer(cc);

            logger.info("init the consumer ok,begin receive data from the topic " + this.topic);

            consumer.subscribe(topic, 2 * 1024 * 1024, new MessageListenerImpl(bufferPool));

            consumer.completeSubscribe();
        } catch (MetaClientException ex) {
            logger.error(ex, ex);
        }
    }

    private class MessageListenerImpl implements MessageListener {

        ArrayBlockingQueue bufferPool = null;

        public MessageListenerImpl(ArrayBlockingQueue bufferPool) {
            this.bufferPool = bufferPool;
        }

        @Override
        public void recieveMessages(Message msg) {
            if (msg != null) {
                while (!bufferPool.offer(msg)) {
                    logger.debug("the  bufferPool is full,and will sleep ...");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                    }
                }
            }
        }

        @Override
        public Executor getExecutor() {
            return null;
        }
    }
}
