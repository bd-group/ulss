/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.shuthandler;

import cn.ac.iie.ulss.match.datahandler.HttpGetDataServer;
import org.apache.log4j.Logger;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 *
 * @author liucuili
 */
public class KillHandler implements SignalHandler {

    public static Logger log = Logger.getLogger(KillHandler.class.getName());

    public KillHandler() {
    }

    public void registerSignal(String signalName) {
        Signal signal = new Signal(signalName);
        Signal.handle(signal, this);
    }

    @Override
    public void handle(Signal signal) {
        log.info("now receive the system signal " + signal.getName() + " " + signal.getNumber() + "\n\n");
        if (signal.getName().equals("TERM")) {
            HttpGetDataServer.stop();
        } else if (signal.getName().equals("INT") || signal.getName().equals("HUP")) {
            HttpGetDataServer.stop();
        } else {
            log.info("can not process the system signal " + signal.toString());
        }
    }

    public static void main(String[] args) {
    }
}
