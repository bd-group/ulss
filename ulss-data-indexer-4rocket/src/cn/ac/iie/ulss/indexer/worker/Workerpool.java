/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

import cn.ac.iie.ulss.indexer.runenvs.GlobalParas;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;

/**
 *
 * @author work
 */
public class Workerpool {

    public static Logger log = Logger.getLogger(Workerpool.class.getName());
    public static ExecutorService dcxpool;
    public static ExecutorService cdrpool;
    public static ExecutorService hlwrzpool;

    public static void initWorkpool() {
        Workerpool.dcxpool = Executors.newFixedThreadPool(GlobalParas.dcxthreadpoolSize);
        Workerpool.cdrpool = Executors.newFixedThreadPool(GlobalParas.cdrthreadpoolSize);
        Workerpool.hlwrzpool = Executors.newFixedThreadPool(GlobalParas.hlwthreadpoolSize);
        log.info("the work pool size is " + hlwrzpool);
    }

    public static void shutWorkpool() {
        Workerpool.dcxpool.shutdown();
        Workerpool.cdrpool.shutdown();
        Workerpool.hlwrzpool.shutdown();
        log.info("shut the work pool ok ...");
    }
}