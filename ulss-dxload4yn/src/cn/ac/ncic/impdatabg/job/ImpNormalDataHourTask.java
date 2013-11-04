/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.job;

import cn.ac.ncic.impdatabg.commons.datafile.DataFile;
import cn.ac.ncic.impdatabg.commons.Line;
import cn.ac.ncic.impdatabg.commons.RuntimeEnv;
import cn.ac.ncic.impdatabg.commons.Stat;
import cn.ac.ncic.impdatabg.util.DateService;
import cn.ac.ncic.impdatabg.util.MessageProcess;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author AlexMu
 */
public class ImpNormalDataHourTask extends ImpDataTask {

    protected String errDataDir;
    protected Date endTime;
    protected List<Line> errlineslst = new ArrayList<Line>();

    public ImpNormalDataHourTask(DataFile pDataFile, String pStatResDir, String pStatDateStr, String pErrDataDir, Date pEndTime) {
        super(pDataFile, pStatResDir, pStatDateStr);
        this.errDataDir = pErrDataDir;
        this.endTime = pEndTime;
    }

    protected void importDataSuccProcess(List<Line> lst) {
        if (lineslst.size() > 0) {
            dataFile.getChkPointFile("succChkpFile").writeChkpoint(lineslst.get(lineslst.size() - 1).currchars);
            succrows += lineslst.size();
        }
    }

    protected void importDataFailProcess(List<Line> lst) {
        //write current lines to file
        if (lineslst.size() > 0) {
            writetoErrDataFile(lineslst);
        }
    }

    void commit() {
        try {
            if (lineslst.size() > 0) {
                if (errlineslst.size() > 0) {
                    if (lineslst.get(lineslst.size() - 1).currchars > errlineslst.get(errlineslst.size() - 1).currchars) {
                        writetoErrDataFile(errlineslst);
                        errlineslst.clear();
                        importData();
                    } else {
                        importData();
                        writetoErrDataFile(errlineslst);
                        errlineslst.clear();
                    }
                } else {
                    importData();
                }
            } else if (errlineslst.size() > 0) {
                writetoErrDataFile(errlineslst);
                errlineslst.clear();
            }
        } catch (Exception ex) {
            logger.warn(ex);
        }
    }

    public void run() {
	if(this.dataType==null){
		logger.debug(dataFile.getFileName()+" can't be recognized ");
		return;
	}

        logger.debug(dataFile.getFileName());

        int readLineTryLimit = (Integer) RuntimeEnv.getParam("readLineTryLimit");
        int waitExitTimesLimit = (Integer) RuntimeEnv.getParam("waitExitTimesLimit");
        boolean messageProcessEnable = RuntimeEnv.messageProcessEnable;
        String dateRegx = (String) RuntimeEnv.getParam("dateRegx");
        int tryEndSymbolTime = 0;
        boolean finished = false;

        BufferedReader br = null;
        try {
            br = new BufferedReader(dataFile.getInputStreamReader(RuntimeEnv.fileEncoding));
            this.currChkPoint = dataFile.getInitChkPoint();
            logger.info("Opening input file " + dataFile.getFileName() + " is succeeded.");
            br.mark(BUFFER_SIZE);
            while (!finished) {
                String line = "";
                while ((line = br.readLine()) != null) {
                    if (line.endsWith(RuntimeEnv.lineEndSymbol)) {
                        totalrows++;
                        currChkPoint += line.length() + 1;
                        br.mark(BUFFER_SIZE);
                        tryEndSymbolTime = 0;

                        String[] target = null;
                        try {
                            target = dataRetriveHelper.getField(line);
                            if (messageProcessEnable) {
                                MessageProcess.processMessage(target);
                            }
                        } catch (Exception ex) {
                            logger.error("Get fields from current line( " + dataFile.getFileName() + ":" + line + " )is failed for " + ex);
                            errrows++;
                            commit();
                            dataFile.getChkPointFile("errChkpFile").writeChkpoint(currChkPoint);
                            dataFile.getChkPointFile("succChkpFile").writeChkpoint(currChkPoint);
                            continue;
                        }

                        try {
                            setData(target);
                            lineslst.add(new Line(line, currChkPoint));
                            if (lineslst.size() >= batchSize) {
                                commit();
                            }
                        } catch (Exception ex) {
                            logger.warn("Setting value is failed for " + ex + ":" + dataFile.getFileName() + ":" + line);
                            errlineslst.add(new Line(line, currChkPoint));
                            if (errlineslst.size() >= batchSize) {
                                commit();
                            }
                            continue;
                        }

                    } else { //not ended with $EOF$
                        Date now = DateService.getCurrentHour(dateRegx);
                        if (now.compareTo(endTime) > 0) {
                            logger.warn("Error line without end symbol:" + dataFile.getFileName() + ":" + line);
                            totalrows++;
                            errrows++;
                        } else {
                            if (lineslst.size() > 0 || errlineslst.size() > 0) {
                                commit();
                            }
                            sleep(2000);//wait for a while,maybe LServer is writing this line
                            tryEndSymbolTime++;// try on $EOF$
                            if (tryEndSymbolTime >= readLineTryLimit) {
                                logger.warn("Error line without end symbol:" + dataFile.getFileName() + ":" + line + ":end try.");
                                br.mark(BUFFER_SIZE);
                                totalrows++;
                                errrows++;
                                currChkPoint += line.length() + 1;
                                dataFile.getChkPointFile("errChkpFile").writeChkpoint(currChkPoint);
                                dataFile.getChkPointFile("succChkpFile").writeChkpoint(currChkPoint);
                                tryEndSymbolTime = 0;
                            } else {
                                logger.debug("current reset times:" + tryEndSymbolTime + "**length of current line is" + line.length());
                                try {
                                    br.reset();
                                } catch (Exception ex) {
                                    logger.warn(ex);
                                    StackTraceElement[] stes = ex.getStackTrace();
                                    for (int i = 0; i < stes.length; i++) {
                                        logger.warn(stes[i].toString());
                                    }
                                }
                            }
                        }
                    }
                }// end while ((line = br.readLine()) != null)

                if (lineslst.size() > 0 || errlineslst.size() > 0) {
                    commit();
                }

                Date now = DateService.getCurrentHour(dateRegx);
                if (now.compareTo(endTime) <= 0) {
                    sleep(2000);//sleep 2 seconds and continue
                } else {
                    if (waitExitTimes < waitExitTimesLimit) {
                        logger.info("ImpdataJob of " + dataFile.getFileName() + " waits to exit for " + (++waitExitTimes));
                        sleep(30000);//sleep 60 seconds and continue
                    } else {
                        finished = true; //the date has expired,so thread exit
                        logger.info("ImpdataJob really exits this time");
                    }
                }
            }// end while(!finished)
        } catch (Exception ex) {
            logger.warn(ex);
            StackTraceElement[] stes = ex.getStackTrace();
            for (int i = 0; i < stes.length; i++) {
                logger.warn(stes[i].toString());
            }

        } finally {
            try {
                if (lineslst.size() > 0 || errlineslst.size() > 0) {
                    commit();
                }
            } catch (Exception ex) {
                logger.warn(ex);
            }

            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception ex) {
//                ex.printStackTrace();
                logger.warn(ex);
            }


            if (finished && (Boolean) RuntimeEnv.getParam("chkPointFileDeletable")) {
                try {
                    if (dataFile.getChkPointFile("succChkpFile").exists()) {
                        dataFile.getChkPointFile("succChkpFile").delete();
                    }
                } catch (Exception ex) {
                    logger.warn(ex);
                }

                try {
                    if (dataFile.getChkPointFile("errChkpFile").exists()) {
                        dataFile.getChkPointFile("errChkpFile").delete();
                    }
                } catch (Exception ex) {
                    logger.warn(ex);
                }
            }
            String summary = "Impdatajob thread exits with " + dataFile.getFileName() + "*totalrows:" + totalrows + "*succrows:" + succrows + "*errrows:" + errrows;
            logger.info(summary);
            Stat.writeToFile(statResDir, statDateStr, dataFile.getFileName(), summary);
        }
    }

    public void writetoErrDataFile(List<Line> lst) {

        if (lst == null || lst.isEmpty()) {
            return;
        }

        errrows += lst.size();

        File errdir = new File(errDataDir);
        if (!errdir.exists()) {
            errdir.mkdirs();
        }

        String errFilePath = errDataDir + File.separator + dataFile.getFileName() + ".err";

        OutputStreamWriter osw = null;
        try {
            osw = new OutputStreamWriter(new FileOutputStream(new File(errFilePath), true), RuntimeEnv.fileEncoding);
            Line line = null;
            if (lst.size() > 0) {
                for (int i = 0; i < lst.size(); i++) {
                    line = lst.get(i);
                    osw.write(line.line + "\n");
                }
                dataFile.getChkPointFile("errChkpFile").writeChkpoint(lst.get(lst.size() - 1).currchars);
            }
            logger.error(dataFile.getFileName() + ":" + lst.size() + ":" + "Error.");
        } catch (Exception ex) {
            logger.warn(ex);
        } finally {
            try {
                if (osw != null) {
                    osw.close();
                }
            } catch (Exception ex) {
                logger.warn(ex);
            }
        }
    }
}
