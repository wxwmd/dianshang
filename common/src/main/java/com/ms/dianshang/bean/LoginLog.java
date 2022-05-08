package com.ms.dianshang.bean;


import lombok.Data;


@Data
public class LoginLog {


    private LoginCommonLog common;
    private LoginStartLog start;
    private String ts;
}
