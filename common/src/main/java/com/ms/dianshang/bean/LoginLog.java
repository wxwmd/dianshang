package com.ms.dianshang.bean;

import lombok.Data;

@Data
class LoginCommonLog{
    private String ar;
    private String ba;
    private String ch;
    private String md;
    private String mid;
    private String os;
    private String uid;
    private String vc;
}

@Data
class LoginStartLog{
    private String entry;
    private int loadingTime;
    private int openAdId;
    private int openAdMs;
    private int openAdSkipMs;
}

@Data
public class LoginLog {
    private LoginCommonLog common;
    private LoginStartLog start;
    private String ts;
}
