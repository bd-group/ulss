/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.commons;

import cn.ac.ncic.impdatabg.job.ImpDataTask;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author AlexMu
 */
public class Stat {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ImpDataTask.class.getName());
    }

    public static synchronized void writeToFile(String pStatDirStr, String pDateStr, String pFileName, String summary) {
        BufferedWriter bw = null;
        File statResFile = new File(pStatDirStr + File.separator + "statres_" + pDateStr);
        File statdir = new File(pStatDirStr);

        if (!statdir.exists()) {
            statdir.mkdirs();
        }
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(statResFile, true)));
            bw.write(summary + "\n");
        } catch (Exception ex) {
            logger.error("writing statistic record of " + pFileName + "is failed for " + ex.getMessage());
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (Exception ex) {
            }
        }
    }
}
