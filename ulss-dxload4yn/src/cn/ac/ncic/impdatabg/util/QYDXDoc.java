/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.ncic.impdatabg.util;

import java.util.Date;

/**
 *
 * @author alexmu
 */
public class QYDXDoc extends Doc {

    String fssj;
    String dxzx_id;
    String fwq_ip;
    String dx_xh;
    String dxx_block_id;
    String ydz;
    String mddz;
    String dxlx;
    String bmlx;
    String dxfl;
    String ydz_sd;
    String mddz_sd;
    String fxr_szd;
    String sxr_szd;
    String fx_dxzx;
    String sx_dxzx;
    String dxnr;

    public QYDXDoc() {
        dxlx = "1";
        bmlx = "1";
        dxfl = "1";
        fx_dxzx = "1";
        sx_dxzx = "1";
    }

    @Override
    public Doc getDocObj() {
        return new QYDXDoc();
    }

    public String getfssj() {
        return fssj;
    }

    public String getdxzx_id() {
        return dxzx_id;
    }

    public String getfwq_ip() {
        return fwq_ip;
    }

    public String getdx_xh() {
        return dx_xh;
    }

    public String getdxx_block_id() {
        return dxx_block_id;
    }

    public String getydz() {
        return ydz;
    }

    public String getmddz() {
        return mddz;
    }

    public String getdxlx() {
        return dxlx;
    }

    public String getbmlx() {
        return bmlx;
    }

    public String getdxfl() {
        return dxfl;
    }

    public String getydz_sd() {
        return ydz_sd;
    }

    public String getmddz_sd() {
        return mddz_sd;
    }

    public String getfxr_szd() {
        return fxr_szd;
    }

    public String getsxr_szd() {
        return sxr_szd;
    }

    public String getfx_dxzx() {
        return fx_dxzx;
    }

    public String getsx_dxzx() {
        return sx_dxzx;
    }

    public String getdxnr() {
        return dxnr;
    }

    public void setfssj(String fssj) {
        this.fssj = fssj;
    }

    public void setdxzx_id(String dxzx_id) {
        this.dxzx_id = dxzx_id;
    }

    public void setfwq_ip(String fwq_ip) {
        this.fwq_ip = fwq_ip;
    }

    public void setdx_xh(String dx_xh) {
        this.dx_xh = dx_xh;
    }

    public void setdxx_block_id(String dxx_block_id) {
        this.dxx_block_id = dxx_block_id;
    }

    public void setydz(String ydz) {
        this.ydz = ydz;
    }

    public void setmddz(String mddz) {
        this.mddz = mddz;
    }

    public void setdxlx(String dxlx) {
        this.dxlx = dxlx;
    }

    public void setbmlx(String bmlx) {
        this.bmlx = bmlx;
    }

    public void setdxfl(String dxfl) {
        this.dxfl = dxfl;
    }

    public void setydz_sd(String ydz_sd) {
        this.ydz_sd = ydz_sd;
    }

    public void setmddz_sd(String mddz_sd) {
        this.mddz_sd = mddz_sd;
    }

    public void setfxr_szd(String fxr_szd) {
        this.fxr_szd = fxr_szd;
    }

    public void setsxr_szd(String sxr_szd) {
        this.sxr_szd = sxr_szd;
    }

    public void setfx_dxzx(String fx_dxzx) {
        this.fx_dxzx = fx_dxzx;
    }

    public void setsx_dxzx(String sx_dxzx) {
        this.sx_dxzx = sx_dxzx;
    }

    public void setdxnr(String dxnr) {
        this.dxnr = dxnr;
    }
}
