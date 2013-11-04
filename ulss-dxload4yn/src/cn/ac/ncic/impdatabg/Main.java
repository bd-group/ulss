/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg;

import cn.ac.ncic.impdatabg.commons.RuntimeEnv;
import cn.ac.ncic.impdatabg.dispatcher.Dispatcher;
import cn.ac.ncic.impdatabg.conf.Configuration;
import cn.ac.ncic.impdatabg.dispatcher.ErrorImpHourDispatcher;
import cn.ac.ncic.impdatabg.dispatcher.NormalImpDayDispatcher;
import cn.ac.ncic.impdatabg.dispatcher.NormalImpHourDispatcher;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author AlexMu
 */
public class Main {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(Dispatcher.class.getName());
    }

    public static void showUsage() {
        System.out.println("Usage:java -jar dist/impdatabg.jar (normal|error|day)");
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        dispatch(args);
    }

    private static void init() {
        //get configuration from config file
        Configuration conf = Configuration.getConfiguration("impdata.properties");
        if (conf == null) {
            logger.info("reading impdata.properties is failed.");
            System.exit(0);
        }
        logger.info("reading impdata.properties is succeeded.");

        //initialize runtime environment
        if (!RuntimeEnv.initialize(conf)) {
            logger.error("initializing runtime environment is failed.");
            System.exit(0);
        }
        logger.info("initializing runtime environment is  succeeded.");
        RuntimeEnv.dumpEnvironment();
    }

    private static void dispatch(String[] args) {
        Dispatcher dispatcher = null;

        if (args.length < 1) {
            showUsage();
            System.exit(0);
        }

        init();
        
        String runMode = args[0];

        if (runMode.isEmpty()) {
            logger.info("please define run mode(normal,error,day).");
            System.exit(0);
        } else {
            logger.info("current run mode:" + runMode);
            if (runMode.equals("normal")) {
                dispatcher = new NormalImpHourDispatcher();
            } else if (runMode.equals("error")) {
                dispatcher = new ErrorImpHourDispatcher();
            } else if (runMode.equals("day")) {
                dispatcher = new NormalImpDayDispatcher();
            } else {
                logger.info("unknown run mode:" + runMode);
                System.exit(0);
            }
            dispatcher.dispatch(args);
        }
    }
}
