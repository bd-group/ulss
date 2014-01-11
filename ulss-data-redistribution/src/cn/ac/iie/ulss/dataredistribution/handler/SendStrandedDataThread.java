/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import cn.ac.iie.ulss.dataredistribution.consistenthashing.RNode;
import cn.ac.iie.ulss.dataredistribution.tools.Rule;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class SendStrandedDataThread implements Runnable {

    ConcurrentLinkedQueue<Object[]> strandedDataSend = null;
    Map<String, ThreadGroup> topicToSendThreadPool = null;
    ThreadGroup sendThreadPool = null;
    static org.apache.log4j.Logger logger = null;
    int sendThreadPoolSize = (Integer) RuntimeEnv.getParam(RuntimeEnv.SEND_THREAD_POOL_SIZE);

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(SendStrandedDataThread.class.getName());
    }

    public SendStrandedDataThread(ConcurrentLinkedQueue<Object[]> strandedDataSend) {
        this.strandedDataSend = strandedDataSend;
    }

    @Override
    public void run() {
        topicToSendThreadPool = (Map<String, ThreadGroup>) RuntimeEnv.getParam(GlobalVariables.TOPIC_TO_SEND_THREADPOOL);

        while (true) {
            Object[] o = strandedDataSend.poll();
            if (o == null) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            } else {
                RNode node = (RNode) o[0];
                String sendIP = (String) o[1];
                String keyinterval = (String) o[2];
                Long f_id = (Long) o[3];
                String road = (String) o[4];
                int count = (Integer) o[5];
                Rule r = (Rule) o[6];
                byte[] data = (byte[]) o[7];

                sendThreadPool = topicToSendThreadPool.get(r.getTopic());
                while (sendThreadPool.activeCount() >= sendThreadPoolSize) {
                    logger.debug("the sendThreadPool for strandedData is full...");
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ex) {
                        logger.error(ex, ex);
                    }
                }
                SendToServiceThread sst = new SendToServiceThread(data, node, r, sendIP, keyinterval, f_id, road, count);
                Thread tsst = new Thread(sendThreadPool, sst);
                tsst.setName("SendToServiceThread-" + r.getTopic() + "-" + node.getName() + "-" + keyinterval);
                tsst.start();
                logger.info("begin to send " + count + " strandedData for " + r.getTopic() + "-" + node.getName() + "-" + keyinterval + " to " + sendIP);
            }
        }
    }
}
