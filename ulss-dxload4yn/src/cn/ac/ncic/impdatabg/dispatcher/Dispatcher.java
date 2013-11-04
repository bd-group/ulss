/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.dispatcher;

import cn.ac.ncic.impdatabg.commons.RuntimeEnv;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author AlexMu
 */
public abstract class Dispatcher {

    Executor exec = null;
    Logger logger = null;

    public Dispatcher() {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(Dispatcher.class.getName());
        logger.info(RuntimeEnv.fileDisposeWorkerNum);
        exec = Executors.newFixedThreadPool(RuntimeEnv.fileDisposeWorkerNum);
    }

    public abstract void dispatch(String[] args);

    void waitAndExit(long timeout) {
        ExecutorService execService = (ExecutorService) exec;
        execService.shutdown();
        try {
            execService.awaitTermination(timeout, TimeUnit.HOURS);
            logger.info(this.getClass().getName() + " waits to exit.");
        } catch (Exception ex) {
            logger.warn(ex.getMessage());
        } finally {
            logger.info(this.getClass().getName() + " exits successfully.");
            System.exit(0);
        }
    }

    void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException intex) {
//                intex.printStackTrace();
            logger.warn(intex.getMessage());
        } catch (Exception ex) {
//                ex.printStackTrace();
            logger.warn(ex.getMessage());
        }
    }
}
