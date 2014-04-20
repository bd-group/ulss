/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.indexer.worker;

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
        dcxpool = Executors.newFixedThreadPool(Indexer.dcxthreadpoolSize);
        cdrpool = Executors.newFixedThreadPool(Indexer.cdrthreadpoolSize);
        hlwrzpool = Executors.newFixedThreadPool(Indexer.hlwrzthreadpoolSize);
        log.info("the work pool size is " + hlwrzpool);
    }
}