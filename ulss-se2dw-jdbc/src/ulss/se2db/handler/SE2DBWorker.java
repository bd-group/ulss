/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ulss.se2db.handler;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
abstract class SE2DBWorker implements Runnable {

    protected TableSe2DBHandler tableSe2DBHandler;
    protected volatile boolean stopped;
    private static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(SE2DBWorker.class.getName());
    }

    public SE2DBWorker(TableSe2DBHandler pTableSe2DBHandler) {
        tableSe2DBHandler = pTableSe2DBHandler;
        stopped = false;
    }

    public void stop() {
        stopped = true;
    }
}
