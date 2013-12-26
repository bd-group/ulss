/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.dataredistribution.handler;

import cn.ac.iie.ulss.dataredistribution.commons.GlobalVariables;
import cn.ac.iie.ulss.dataredistribution.commons.RuntimeEnv;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author evan
 */
public class StoreUselessDataThread implements Runnable {

    String topic = null;
    ConcurrentLinkedQueue sdQueue = null;
    static org.apache.log4j.Logger logger = null;
    String dataDir = (String) RuntimeEnv.getParam(RuntimeEnv.DATA_DIR);
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = org.apache.log4j.Logger.getLogger(StoreStrandedDataThread.class.getName());
    }

    public StoreUselessDataThread(ConcurrentLinkedQueue sdQueue, String topic) {
        this.sdQueue = sdQueue;
        this.topic = topic;
    }

    public void run() {
        int count = 0;
        int count2 = 0;
        File out = new File(dataDir + "useless");
        File f = null;
        while (true) {
            if (!sdQueue.isEmpty()) {
                count2 = 0;
                synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_DIR)) {
                    if (!out.exists() && !out.isDirectory()) {
                        out.mkdirs();
                        logger.info("create the directory " + dataDir + "useless");
                    }
                }

                f = new File(dataDir + "useless/" + topic + ".ul");

                count = 0;

                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(f, true);
                } catch (FileNotFoundException ex) {
                    logger.error(ex, ex);
                    continue;
                }
                BufferedOutputStream bos = new BufferedOutputStream(fos);

                while (count < 1000) {
                    count++;
                    if (!sdQueue.isEmpty()) {
                        byte[] b = (byte[]) sdQueue.poll();
                        if (b == null) {   
                            continue;
                        }

                        try {
                            bos.write(b);
                            bos.write('\n');
                        } catch (IOException ex) {
                            logger.error(b.toString() + ex, ex);
                            continue;
                        }
                        if (count % 100 == 0) {
                            try {
                                fos.flush();
                            } catch (IOException ex) {
                                logger.error(ex, ex);
                            }
                        }
                    } else {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ex) {
                            logger.error(ex, ex);
                        }
                    }
                }
                try {
                    bos.flush();
                    bos.close();
                } catch (IOException ex) {
                    logger.error(ex, ex);
                }

                Date d = new Date();
                String fb = format.format(d) + "_" + f.getName();
                if (f.exists()) {
                    f.renameTo(new File(dataDir + "useless/" + fb));
                }
            } else {
                count2++;
                if (count2 >= 300) {
                    ConcurrentHashMap<String, ConcurrentLinkedQueue> uselessDataStore = (ConcurrentHashMap<String, ConcurrentLinkedQueue>) RuntimeEnv.getParam(GlobalVariables.USELESS_DATA_STORE);
                    synchronized (RuntimeEnv.getParam(GlobalVariables.SYN_STORE_USELESSDATA)) {
                        uselessDataStore.remove(topic);
                    }
                    break;
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    logger.error(ex, ex);
                }
            }
        }
    }
}
