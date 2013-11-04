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
public class ImpNormalDataDayTask extends ImpDataTask {

    public ImpNormalDataDayTask(DataFile pDataFile, String pStatResDir, String pStatDateStr) {
        super(pDataFile, pStatResDir, pStatDateStr);
    }

    protected void importDataSuccProcess(List<Line> lst) {
        if (lineslst.size() > 0) {
            dataFile.getChkPointFile("succChkpFile").writeChkpoint(lineslst.get(lineslst.size() - 1).currchars);
            succrows += lineslst.size();
        }
    }

    protected void importDataFailProcess(List<Line> lst) {
        if (lineslst.size() > 0) {
            dataFile.getChkPointFile("errChkpFile").writeChkpoint(lineslst.get(lineslst.size() - 1).currchars);
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
                        logger.error("Get fields from current line( " + dataFile.getFileName() + ":" + line + " )is failed for " + ex.getMessage());
                        errrows++;
                        if (lineslst.size() > 0) {
                            importData();
                        }
                        dataFile.getChkPointFile("succChkpFile").writeChkpoint(currChkPoint);
                        dataFile.getChkPointFile("errChkpFile").writeChkpoint(currChkPoint);
                        continue;
                    }


                    setData(target);
                    lineslst.add(new Line(line, currChkPoint));

                    if (lineslst.size() >= batchSize) {
                        importData();
                    }

                } else { //not ended with $EOF$
                    logger.warn("Error line without end symbol:" + dataFile.getFileName() + ":" + line);
                    errrows++;
                    if (lineslst.size() > 0) {
                        importData();
                    }
                    dataFile.getChkPointFile("succChkpFile").writeChkpoint(currChkPoint);
                    dataFile.getChkPointFile("errChkpFile").writeChkpoint(currChkPoint);
                }
            }// end while ((line = br.readLine()) != null)
           
        } catch (Exception ex) {
//            ex.printStackTrace();
            logger.warn(ex.getMessage());
            StackTraceElement[] stes = ex.getStackTrace();
            for (int i = 0; i < stes.length; i++) {
                logger.warn(stes[i].toString());
            }

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
//                ex.printStackTrace();
                logger.warn(ex.getMessage());
            }

            if ((Boolean) RuntimeEnv.getParam("chkPointFileDeletable")) {
                try {
                    if (dataFile.getChkPointFile("succChkpFile").exists()) {
                        dataFile.getChkPointFile("succChkpFile").delete();
                    }
                } catch (Exception ex) {
                    logger.warn(ex.getMessage());
                }

                try {
                    if (dataFile.getChkPointFile("errChkpFile").exists()) {
                        dataFile.getChkPointFile("errChkpFile").delete();
                    }
                } catch (Exception ex) {
                    logger.warn(ex.getMessage());
                }
            }

            logger.info("Impdatajob thread exits with " + dataFile.getFileName() + "*totalrows:" + totalrows + "*succrows:" + succrows + "*errrows:" + errrows);
        }
    }
}
