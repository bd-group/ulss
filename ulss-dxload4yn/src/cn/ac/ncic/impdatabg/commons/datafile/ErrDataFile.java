/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.commons.datafile;

import java.io.File;

/**
 *
 * @author AlexMu
 */
public class ErrDataFile extends DataFile {

    public ErrDataFile(File pFile, File pChkPointDir) {
        super(pFile, pChkPointDir);
        chkPointFiles.put("cmtChkpFile", new CheckPointFile(new File(checkPointDir + File.separator + this.file.getName() + ".cmt.chkp")));
    }
}
