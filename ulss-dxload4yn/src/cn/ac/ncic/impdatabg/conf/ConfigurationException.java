/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.conf;

/**
 *
 * @author AlexMu
 */
public class ConfigurationException extends Exception {

    public ConfigurationException() {
        super();
    }

    public ConfigurationException(String msg) {
        super(msg);
    }

    public ConfigurationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public ConfigurationException(Throwable cause) {
        super(cause);
    }
}
