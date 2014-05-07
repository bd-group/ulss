/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
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
                if (HttpDataHandler.activethreadNum.get() >= 10 || count >= 5) {
                    log.info("now the httpserver threadpool,the active thread num is " + HttpDataHandler.activethreadNum.get() + ",the file write at the same time is: " + GlobalParas.id2Createindex.size());
                    count = 0;
                }

                for (Long file_id : GlobalParas.metaORdeviceErrorFileId.keySet()) {
                    if (System.currentTimeMillis() - GlobalParas.metaORdeviceErrorFileId.get(file_id) >= 1000 * GlobalParas.badfileinfoReserveSeconds) {
                        GlobalParas.metaORdeviceErrorFileId.remove(file_id);
                        log.info("remove file id " + file_id + ",now the map is " + GlobalParas.metaORdeviceErrorFileId);
                    }
                }
                for (Long file_id : GlobalParas.removedFileId.keySet()) {
                    if (System.currentTimeMillis() - GlobalParas.removedFileId.get(file_id) >= 1000 * GlobalParas.badfileinfoReserveSeconds) {
                        GlobalParas.removedFileId.remove(file_id);
                        log.info("remove file id " + file_id + ",now the map is " + GlobalParas.removedFileId);
                    }
                }
                for (Long file_id : GlobalParas.diskErrorFileId.keySet()) {
                    if (System.currentTimeMillis() - GlobalParas.diskErrorFileId.get(file_id) >= 1000 * GlobalParas.badfileinfoReserveSeconds) {
                        GlobalParas.diskErrorFileId.remove(file_id);
                        log.info("remove file id " + file_id + ",now the map is " + GlobalParas.diskErrorFileId);
                    }
                }

                if (GlobalParas.isShouldExit.get()) {
                    break;
                }
            } catch (Exception ex) {
                log.error(ex, ex);
            }
        }
    }
}
