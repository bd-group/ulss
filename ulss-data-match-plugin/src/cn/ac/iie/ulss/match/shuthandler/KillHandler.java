/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.match.shuthandler;

import cn.ac.iie.ulss.match.datahandler.HttpGetDataServer;
import cn.ac.iie.ulss.match.worker.Matcher;
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
            for (String s : Matcher.cdrupdaters.keySet()) {
                log.info("after receive the kill signal,will dump the position data for " + s);
                Matcher.cdrupdaters.get(s).isDumping.set(true);
            }
            boolean isShouldExit = false;
            while (!isShouldExit) {
                try {
                    Thread.sleep(5000);
                    log.info("check the dump is over ...");
                    isShouldExit = true;
                    for (String s : Matcher.cdrupdaters.keySet()) {
                        if (Matcher.cdrupdaters.get(s).isDumping.get()) {
                            isShouldExit = false;
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error(e, e);
                }
            }
            Matcher.isShouldExit.set(true);
            log.info("after receive the kill signal,will stop the http server ");
            HttpGetDataServer.stop();
            log.info("stop the http server ok ");
        } else if (signal.getName().equals("INT") || signal.getName().equals("HUP")) {
            for (String s : Matcher.cdrupdaters.keySet()) {
                log.info("after receive the kill signal,will dump the position data for " + s);
                Matcher.cdrupdaters.get(s).isDumping.set(true);
            }
            boolean isShouldEStopcheck = false;
            while (!isShouldEStopcheck) {
                try {
                    Thread.sleep(5000);
                    log.info("check the dump is over ...");
                    isShouldEStopcheck = true;
                    for (String s : Matcher.cdrupdaters.keySet()) {
                        if (Matcher.cdrupdaters.get(s).isDumping.get()) {
                            isShouldEStopcheck = false;
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error(e, e);
                }
            }
            Matcher.isShouldExit.set(true);
            HttpGetDataServer.stop();
        } else {
            log.info("can not process the system signal " + signal.toString());
        }
    }

    public static void main(String[] args) {
    }
}
