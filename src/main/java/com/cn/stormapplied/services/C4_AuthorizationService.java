package com.cn.stormapplied.services;

import java.io.Serializable;

public class C4_AuthorizationService implements Serializable {
    public boolean authorize(C4_Order order) {
        return true;
    }
}
