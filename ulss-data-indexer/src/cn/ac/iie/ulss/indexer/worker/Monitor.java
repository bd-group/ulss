/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import org.apache.log4j.Logger;

/**
 *
 * @author work
 */
public class Monitor implements Runnable {

    public static Logger log = Logger.getLogger(Monitor.class.getName());

    @Override
    public void run() {
        int count = 0;
        while (true) {
            try {
                count++;
                Thread.sleep(2000);
                if (HttpDataHandler.activethreadNum.get() >= 10 || count >= 3) {
                    log.info("now the httpserver threadpool,the active thread num is " + HttpDataHandler.activethreadNum.get());
                    count = 0;
                }
                if (Indexer.isShouldExit.get()) {
                    break;
                }
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }
    }
}