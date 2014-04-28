/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.ulss.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author liucuili
 */
public class Configure {

    String cfgFile;
    Properties proCfg;

    public Configure() throws FileNotFoundException, IOException {
        cfgFile = "config.properties";
        System.out.println(this.cfgFile);
        FileInputStream inStream = new FileInputStream(this.cfgFile);
        proCfg = new Properties();
        proCfg.load(inStream);
    }

    public Configure(String f) throws FileNotFoundException, IOException {
        cfgFile = f;
        FileInputStream inStream = new FileInputStream(this.cfgFile);
        proCfg = new Properties();
        proCfg.load(inStream);
    }

    public String getProperty(String k) {
        return proCfg.getProperty(k);
    }

    public int getIntProperty(String k) {
        return Integer.parseInt(proCfg.getProperty(k));
    }

    public long getLongProperty(String k) {
        return Long.parseLong(proCfg.getProperty(k));
    }

    public Boolean getBooleanProperty(String k) {
        return false;
    }
}
