/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.dispatcher;

import cn.ac.ncic.impdatabg.commons.RuntimeEnv;
import cn.ac.ncic.impdatabg.commons.DirGroup;
import cn.ac.ncic.impdatabg.commons.datafile.NormalDataFile;
import cn.ac.ncic.impdatabg.job.ImpNormalDataHourTask;
import cn.ac.ncic.impdatabg.util.DateService;
import cn.ac.ncic.impdatabg.util.FileService;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;

/**
 *
 * @author AlexMu
 */
public class NormalImpHourDispatcher extends Dispatcher {

    public NormalImpHourDispatcher() {
        super();
        RuntimeEnv.addParam("readLineTryLimit", 10);
        RuntimeEnv.addParam("waitExitTimesLimit", 10);
        RuntimeEnv.addParam("chkPointFileDeletable", false);
        RuntimeEnv.addParam("dateRegx", "yyyyMMddHH");
    }

    public void dispatch(String[] args) {
        String dateRegx = (String) RuntimeEnv.getParam("dateRegx");
        Date currentHour = DateService.getCurrentHour(dateRegx);
        if (currentHour == null) {
            return;
        }
        Date now = null;

        List<DirGroup> dirGroupLst = RuntimeEnv.dirGroupLst;
        Map<String, List<File>> dir2FileLst = new HashMap<String, List<File>>();
        String fileNameRegx = RuntimeEnv.fileNamePrefix + "*" + DateService.getTimeStr(currentHour, dateRegx) + "*";
        while (true) {
            try {
                for (int i = 0; i < dirGroupLst.size(); i++) {
                    String dir = dirGroupLst.get(i).getDir();
                    List<File> newFilelst = FileService.getFiles(dir, fileNameRegx);

                    if (newFilelst != null) {
                        List currFileLst = dir2FileLst.get(dir);
                        if (currFileLst == null) {
                            dir2FileLst.put(dir, newFilelst);
                            currFileLst = newFilelst;
                            for (int k = 0; k < currFileLst.size(); k++) {
                                //dispose the new file with a new thread
                                exec.execute(new ImpNormalDataHourTask(new NormalDataFile((File) currFileLst.get(k), new File(dirGroupLst.get(i).getChkpointdir())), dirGroupLst.get(i).getStatdir(), DateService.getTimeStr(currentHour, "yyyyMMdd"), dirGroupLst.get(i).getErrdir(), currentHour));
                            }
                        } else {
                            if (currFileLst.size() != newFilelst.size()) {
                                Collection sub = CollectionUtils.subtract(newFilelst, currFileLst);
                                List extraFilelst = Arrays.asList(sub.toArray());
                                for (int k = 0; k < extraFilelst.size(); k++) {
                                    currFileLst.add((File) extraFilelst.get(k));
                                    //dispose the new file with a new thread
                                    exec.execute(new ImpNormalDataHourTask(new NormalDataFile((File) extraFilelst.get(k), new File(dirGroupLst.get(i).getChkpointdir())), dirGroupLst.get(i).getStatdir(), DateService.getTimeStr(currentHour, "yyyyMMdd"), dirGroupLst.get(i).getErrdir(), currentHour));
                                }
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                logger.warn(ex.getMessage());
            }

            now = DateService.getCurrentHour(dateRegx);

            if (now.compareTo(currentHour) > 0) {
                //another one hour and main thread waits to exit
                //only wait for sub threads to exit for 3 hour at most
                waitAndExit(1);
            }

            sleep(10000);

        }// end while
    }
}
