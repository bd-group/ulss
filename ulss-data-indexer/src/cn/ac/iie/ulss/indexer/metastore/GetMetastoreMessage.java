/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.metastore;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils;
import java.text.SimpleDateFormat;
import java.util.concurrent.Executor;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author liucuili
 */
public class GetMetastoreMessage implements Runnable {

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static Logger log = null;
    int attempSize = 2;

    static {
        PropertyConfigurator.configure("log4j.properties");
    }

    @Override
    public void run() {
        log.info("getting the message from the metastore");
        String mszkurl = "";
        String zkUrl = mszkurl.split("\\|")[0];
        String topic = mszkurl.split("\\|")[1];
        String ccgroup = mszkurl.split("\\|")[2];
        MetaClientConfig metaClientConfig = new MetaClientConfig();
        final ZkUtils.ZKConfig zkConfig = new ZkUtils.ZKConfig();
        zkConfig.zkConnect = zkUrl;
        metaClientConfig.setZkConfig(zkConfig);
        final MessageSessionFactory sessionFactory;
        try {
            sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
            ConsumerConfig cc = new ConsumerConfig("persi " + ccgroup + "_consumer");
            cc.setFetchRunnerCount(1);
            final MessageConsumer consumer = sessionFactory.createConsumer(cc);
            log.info("init the consumer ok,begin receive data from the metastore ");
            consumer.subscribe(topic, 5 * 1024 * 1024, new MessageListener() {
                @Override
                public void recieveMessages(Message message) {
                    DDLMsg msg = new DDLMsg();
                    String data = new String(message.getData());
                    msg = DDLMsg.fromJson(data);
                    int eventId = (int) msg.getEvent_id();
                    String db_name = null;
                    String table_name = null;
                    String column_name = null;
                    String column_type = null;
                    Long version = -1L;

                    switch (eventId) {
                        case 1101: // 新建表
                            log.info("the message from metastore for 1101!!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            break;

                        case 1102: // 修改表名
                            log.info("the message from metastore for 1102!!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            String old_table_name = (String) msg.getMsg_data().get("old_table_name");
                            break;

                        case 1105: // 划分规则改变
                            log.info("the message from metastore for 1105!!! , begin to change the transmit rule");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            version = Long.parseLong((String) msg.getMsg_data().get("version"));
                            break;

                        case 1201: // 删除列
                            log.info("the message from metastore for 1201!!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            column_name = (String) msg.getMsg_data().get("column_name");
                            column_type = (String) msg.getMsg_data().get("column_type");
                            break;

                        case 1202: //新增列
                            log.info("the message from metastore for 1202 !!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            column_name = (String) msg.getMsg_data().get("column_name");
                            column_type = (String) msg.getMsg_data().get("column_type");
                            break;

                        case 1203: //修改列名
                            log.info("the message from metastore for 1203 !!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            column_name = (String) msg.getMsg_data().get("column_name");
                            String old_column_name = (String) msg.getMsg_data().get("old_column_name");
                            break;
                        case 1207: //删除表
                            log.info("the message from metastore for 1207 !!!");
                            db_name = (String) msg.getMsg_data().get("db_name");
                            table_name = (String) msg.getMsg_data().get("table_name");
                            break;
                        default:
                            log.info("Event " + eventId + " is useless");
                            break;
                    }
                }

                @Override
                public Executor getExecutor() {
                    return null;
                }
            });

            consumer.completeSubscribe();
        } catch (MetaClientException ex) {
            log.error(ex, ex);
        }
    }
}