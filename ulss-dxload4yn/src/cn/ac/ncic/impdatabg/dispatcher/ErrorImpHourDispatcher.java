/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.dispatcher;

import cn.ac.ncic.impdatabg.commons.DirGroup;
import cn.ac.ncic.impdatabg.commons.RuntimeEnv;
import cn.ac.ncic.impdatabg.commons.datafile.ErrDataFile;
import cn.ac.ncic.impdatabg.job.ImpErrDataHourTask;
import cn.ac.ncic.impdatabg.util.DateService;
import cn.ac.ncic.impdatabg.util.FileService;
import java.io.File;
import java.util.Date;
import java.util.List;

/**
 *
 * @author AlexMu
 */
public class ErrorImpHourDispatcher extends Dispatcher {

    public ErrorImpHourDispatcher() {
        super();
        RuntimeEnv.addParam("dateRegx", "yyyyMMddHH");
    }

    public void dispatch(String[] args) {
        String dateRegx = (String) RuntimeEnv.getParam("dateRegx");
        Date lastHour = DateService.getLastHour(dateRegx);
        if (lastHour == null) {
            return;
        }
        List<DirGroup> dirGroupLst = RuntimeEnv.dirGroupLst;
        List<File> fileLst = null;
        String fileNameRegx = RuntimeEnv.fileNamePrefix + "*" + DateService.getTimeStr(lastHour, dateRegx) + "*";

        try {

            for (int i = 0; i < dirGroupLst.size(); i++) {
                String errdir = dirGroupLst.get(i).getErrdir();
                fileLst = FileService.getFiles(errdir, fileNameRegx);
                if (fileLst == null) {
                    logger.info("There is no error file in " + errdir);
                    continue;
                } else {
                    for (int k = 0; k < fileLst.size(); k++) {
                        exec.execute(new ImpErrDataHourTask(new ErrDataFile((File) fileLst.get(k), new File(dirGroupLst.get(i).getChkpointdir())), dirGroupLst.get(i).getStatdir(), DateService.getTimeStr(lastHour, "yyyyMMdd")));
                    }//for
                }//else
            }
            //waits for one hour
            waitAndExit(1);
        } catch (Exception ex) {
//            ex.printStackTrace();
            logger.warn(ex.getMessage());
        }
    }
}
