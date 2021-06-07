package com.atguigu.gmall_publisher.esdao;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;

/**
 * Created by VULCAN on 2021/6/2
 */
public interface ESDao {

    JSONObject getOrderDetail(String date, Integer startpage , Integer size, String keyword) throws IOException;

}
