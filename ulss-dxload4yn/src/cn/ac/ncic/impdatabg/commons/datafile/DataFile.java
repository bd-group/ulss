/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.commons.datafile;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author AlexMu
 */
public abstract class DataFile {

    File file;
    File checkPointDir;
    long initChkPoint = 0;
    Map<String, CheckPointFile> chkPointFiles = new HashMap<String, CheckPointFile>();

    public DataFile(File pFile, File pCheckPointDir) {
        this.file = pFile;
        this.checkPointDir = pCheckPointDir;
        if (!this.checkPointDir.exists()) {
            this.checkPointDir.mkdirs();
        }
    }

    public InputStreamReader getInputStreamReader(String pCharsetName) throws Exception {
        InputStreamReader isr = null;
        long currChkPoint = 0;
        long maxChkPoint = 0;

        Set chkPointFileSet = chkPointFiles.keySet();
        Iterator itr = chkPointFileSet.iterator();
        while (itr.hasNext()) {
            currChkPoint = ((CheckPointFile) chkPointFiles.get((String) itr.next())).getCheckPoint();
            maxChkPoint = currChkPoint > maxChkPoint ? currChkPoint : maxChkPoint;
        }

        this.initChkPoint = maxChkPoint;
        isr = new InputStreamReader(new FileInputStream(this.file), pCharsetName);
        isr.skip(maxChkPoint);

        return isr;
    }

    public String getFileName() {
        return this.file.getName();
    }

    public CheckPointFile getChkPointFile(String pChkPointFileName) {
        return (CheckPointFile) chkPointFiles.get(pChkPointFileName);
    }

    public long getInitChkPoint() {
        return initChkPoint;
    }
}
