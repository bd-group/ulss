/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.job;

import cn.ac.ncic.impdatabg.commons.Line;
import cn.ac.ncic.impdatabg.commons.RuntimeEnv;
import cn.ac.ncic.impdatabg.commons.datafile.DataFile;
import cn.ac.ncic.impdatabg.util.MessageProcess;
import java.io.BufferedReader;
import java.util.List;

/**
 *
 * @author AlexMu
 */
public class ImpErrDataHourTask extends ImpDataTask {

    public ImpErrDataHourTask(DataFile pDataFile, String pStatResDir, String pStatDateStr) {
        super(pDataFile, pStatResDir, pStatDateStr);
    }

    protected void importDataSuccProcess(List<Line> lst) {
        if (lineslst.size() > 0) {
            dataFile.getChkPointFile("cmtChkpFile").writeChkpoint(lineslst.get(lineslst.size() - 1).currchars);
            succrows += lineslst.size();
        }
    }

    protected void importDataFailProcess(List<Line> lst) {
        //write current lines to file
        if (lineslst.size() > 0) {
            dataFile.getChkPointFile("cmtChkpFile").writeChkpoint(lineslst.get(lineslst.size() - 1).currchars);
            errrows += lineslst.size();
            for (int i = 0; i < lineslst.size(); i++) {
                logger.debug("Error line:" + lineslst.get(i).line);
            }
        }
    }

    public void run() {
	if(this.dataType==null){
                logger.debug(dataFile.getFileName()+" can't be recognized ");
                return;
        }
        logger.debug(dataFile.getFileName());
        
        this.batchSize = 1;//very special
        
        BufferedReader br = null;
        try {
            br = new BufferedReader(dataFile.getInputStreamReader(RuntimeEnv.fileEncoding));
            this.currChkPoint = this.dataFile.getInitChkPoint();
            logger.info("Opening input file " + dataFile.getFileName() + " is succeeded.");
            String line = "";
            while ((line = br.readLine()) != null) {
                totalrows++;
                currChkPoint += line.length() + 1;
                if (line.endsWith(RuntimeEnv.lineEndSymbol)) {

                    String[] target = null;
                    try {
                        target = dataRetriveHelper.getField(line);
                        if (RuntimeEnv.messageProcessEnable) {
                            MessageProcess.processMessage(target);
                        }
                    } catch (Exception ex) {
                        logger.error("Get fields from current line( " + line + " )is failed for " + ex.getMessage());
                        errrows++;
                        if (lineslst.size() > 0) {
                            importData();
                        }
                        continue;
                    }

                    setData(target);
                    lineslst.add(new Line(line, currChkPoint));
                    if (lineslst.size() >= batchSize) {
                        importData();
                    }

                } else { //not ended with $EOF$
                    logger.warn("Error line without end symbol:" + line);
                    errrows++;
                    if (lineslst.size() > 0) {
                        importData();
                    }
                    dataFile.getChkPointFile("cmtChkpFile").writeChkpoint(currChkPoint);
                }
            }// end while ((line = br.readLine()) != null)
        } catch (Exception ex) {
            logger.warn(ex.getMessage());
        } finally {
            try {
                if (lineslst.size() > 0) {
                    importData();
                }
            } catch (Exception ex) {
                logger.warn(ex.getMessage());
            }

            try {
                br.close();
            } catch (Exception ex) {
                logger.warn(ex.getMessage());
            }

            // can't delete chkpoint file,or this program will execute repeatedly for a file mwm 2010-06-22 15:27
            logger.info("Impdatajob thread exits with " + dataFile.getFileName() + "*totalrows:" + totalrows + "*succrows:" + succrows + "*errrows:" + errrows);
        }
    }
}
