package com.ms.dianshang.bean;

import lombok.Data;

@Data
public class LoginStartLog{
    private String entry;
    private int loadingTime;
    private int openAdId;
    private int openAdMs;
    private int openAdSkipMs;
}
