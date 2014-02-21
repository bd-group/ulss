package ulss.data.statistics;

import cn.ac.iie.ulss.statistics.commons.GlobalVariables;
import cn.ac.iie.ulss.statistics.commons.RuntimeEnv;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author evan yang
 */
public class DataAccepterThread implements Runnable {

    public SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String zkUrl = null;
    String MQ = null;
    String time = null;
    HashMap<String, AtomicLong[]> timeToCount = null;
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(DataAccepterThread.class.getName());
    }

    public DataAccepterThread(String zkUrl, String MQ, String time , HashMap<String, AtomicLong[]> timeToCount) {
        this.zkUrl = zkUrl;
        this.MQ = MQ;
        this.time = time;
        this.timeToCount = timeToCount;
    }

    @Override
    public void run() {
        pullDataFromQ();
    }

    /**
     *
     * 根据表的名字获得对应的消息队列的名字，然后根据消息队列的分区从中拉取数据,然后放入bufferPool中
     */
    public void pullDataFromQ() {
        Map<String, MessageConsumer> MQToConsumer = (Map<String, MessageConsumer>) RuntimeEnv.getParam(GlobalVariables.MQ_TO_CONSUMER);
        
        System.setProperty("notify.remoting.max_read_buffer_size", "10485760");
        MetaClientConfig metaClientConfig = new MetaClientConfig();
        final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
        zkConfig.zkConnect = this.zkUrl;
        metaClientConfig.setZkConfig(zkConfig);
        final MessageSessionFactory sessionFactory;
        try {
            sessionFactory = new MetaMessageSessionFactory(metaClientConfig);

            ConsumerConfig cc = new ConsumerConfig("sta_" + MQ + "_consumer");
            cc.setFetchRunnerCount(1);
            
            final MessageConsumer consumer = sessionFactory.createConsumer(cc);

            logger.info("init the consumer ok,begin receive data from the topic " + this.MQ);

            SplitAndGet sag = new SplitAndGet(MQ , time , timeToCount);
            
            consumer.subscribe(MQ, 2 * 1024 * 1024, new MessageListenerImpl(sag));

            consumer.completeSubscribe();
            
            MQToConsumer.put(MQ, consumer);
            
        } catch (MetaClientException ex) {
            logger.error(ex, ex);
        }
    }

    /**
     *
     * the messageListener use control the flow 
     */
    private class MessageListenerImpl implements MessageListener {

        SplitAndGet sag = null;
        
        public MessageListenerImpl(SplitAndGet sag) {
            this.sag = sag;
        }

        @Override
        public void recieveMessages(Message msg) {
            if (msg != null) {
                sag.count(msg);
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException ex) {
                //logger.error(ex,ex);
            }
        }

        @Override
        public Executor getExecutor() {
            return null;
        }
    }
}
