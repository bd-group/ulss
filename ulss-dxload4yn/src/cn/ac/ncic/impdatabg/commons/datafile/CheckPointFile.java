/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.commons.datafile;

import cn.ac.ncic.impdatabg.job.ImpDataTask;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author AlexMu
 */
public class CheckPointFile {

    File file = null;
    //log
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(CheckPointFile.class.getName());
    }

    public CheckPointFile(File f) {
        this.file = f;
    }

    public long getCheckPoint() {
        long chars = 0;        

        if (!this.file.exists()) {
            return 0;
        }

        String charsStr = getLastLine();
        if (charsStr == null) {
            chars = -1;
        } else {
            try {
                if (charsStr.trim().isEmpty()) {
                    chars = 0;
                } else {
                    chars = Long.parseLong(charsStr);
                }
            } catch (Exception ex) {
                logger.error("failed to parse check point of file " + this.file + " for " + ex);
                chars = -1;
            }
        }

        return chars;
    }

    //get the last line of current checkpoint file
    public String getLastLine() {
        String line = "";
        String linetmp = "";

        BufferedReader br = null;

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            while (true) {
                linetmp = br.readLine();
                if (linetmp == null) {
                    break;
                }
                line = linetmp;
            }
        } catch (Exception ex) {
            logger.error("errors happens when reading check point file " + this.file + " for " + ex);
            return null;
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception innerex) {
            }
        }
        return line;
    }

    public void writeChkpoint(long currchars) {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.file, true)));
            bw.write(String.valueOf(currchars) + "\n");
        } catch (Exception ex) {
            logger.warn(ex);
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (Exception ex) {
            }
        }
    }

    public void delete() {
        this.file.delete();
    }

    public boolean exists() {
        return this.file.exists();
    }
}
