/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.commons;

import java.io.File;

/**
 *
 * @author AlexMu
 */
public class DirGroup {

    String dir;
    String errdir;
    String chkpointdir;
    String statdir;

    public DirGroup(String pdir, String perrdir, String pchkpointdir, String pstatdir) {
        this.dir = pdir;

        if (perrdir == null || perrdir.equals("")) {
            this.errdir = dir + File.separator + "errdata";
        } else {
            this.errdir = perrdir;
        }

        if (pchkpointdir == null || pchkpointdir.equals("")) {
            this.chkpointdir = dir + File.separator + "chkpoint";
        } else {
            this.chkpointdir = pchkpointdir;
        }

        if (pstatdir == null || pstatdir.equals("")) {
            this.statdir = dir + File.separator + "statres";
        } else {
            this.statdir = pstatdir;
        }
    }

    public String getInfo() {
        return this.dir + ":" + this.errdir + ":" + this.chkpointdir + ":" + this.statdir;
    }

    public String getChkpointdir() {
        return chkpointdir;
    }

    public String getDir() {
        return dir;
    }

    public String getErrdir() {
        return errdir;
    }

    public String getStatdir() {
        return statdir;
    }
}
