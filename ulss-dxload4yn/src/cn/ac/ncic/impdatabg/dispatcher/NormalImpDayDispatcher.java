/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.dispatcher;

import cn.ac.ncic.impdatabg.commons.DirGroup;
import cn.ac.ncic.impdatabg.commons.RuntimeEnv;
import cn.ac.ncic.impdatabg.commons.datafile.NormalDataFile;
import cn.ac.ncic.impdatabg.job.ImpNormalDataDayTask;
import cn.ac.ncic.impdatabg.util.DateService;
import cn.ac.ncic.impdatabg.util.FileService;
import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author AlexMu
 */
public class NormalImpDayDispatcher extends Dispatcher {

    public NormalImpDayDispatcher() {
        super();
        RuntimeEnv.addParam("dateRegx", "yyyyMMdd");
        RuntimeEnv.addParam("chkPointFileDeletable", true);
    }

    public void dispatch(String[] args) {
        String dateRegx = (String) RuntimeEnv.getParam("dateRegx");
        Date yesterday = DateService.getTime(Calendar.DAY_OF_MONTH, -1, dateRegx);
        if (yesterday == null) {
            return;
        }

        List<DirGroup> dirGroupLst = RuntimeEnv.dirGroupLst;
        List<File> fileLst = null;
        String fileNameRegx = RuntimeEnv.fileNamePrefix + "*" + DateService.getTimeStr(yesterday, dateRegx) + "*";

        try {

            for (int i = 0; i < dirGroupLst.size(); i++) {
                String dir = dirGroupLst.get(i).getDir();
                fileLst = FileService.getFiles(dir, fileNameRegx);
                if (fileLst == null) {
                    logger.info("There is no file in " + dir);
                    continue;
                } else {
                    for (int k = 0; k < fileLst.size(); k++) {
                        exec.execute(new ImpNormalDataDayTask(new NormalDataFile((File) fileLst.get(k), new File(dirGroupLst.get(i).getChkpointdir())), dirGroupLst.get(i).getStatdir(), DateService.getTimeStr(yesterday, "yyyyMMdd")));
                    }//for
                }//else
            }

            logger.info(NormalImpDayDispatcher.class.getName() + " waits to exits.");
            waitAndExit(1);
        } catch (Exception ex) {
//            ex.printStackTrace();
            logger.warn(ex.getMessage());
        }
    }
}
