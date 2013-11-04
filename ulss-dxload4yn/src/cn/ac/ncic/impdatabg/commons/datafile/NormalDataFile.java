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
public class NormalDataFile extends DataFile {

    public NormalDataFile(File pFile, File pChkPointDir) {
        super(pFile, pChkPointDir);
        chkPointFiles.put("succChkpFile", new CheckPointFile(new File(checkPointDir + File.separator + this.file.getName() + ".succ.chkp")));
        chkPointFiles.put("errChkpFile", new CheckPointFile(new File(checkPointDir + File.separator + this.file.getName() + ".err.chkp")));
    }    
}
