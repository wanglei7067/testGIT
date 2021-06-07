package com.atguigu.gmall_publisher.service;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_publisher.beans.DauPerHour;
import com.atguigu.gmall_publisher.beans.GmvPerHour;

import java.io.IOException;
import java.util.List;

/**
 * Created by VULCAN on 2021/5/28
 */
public interface PublisherService {

    // dau需求
        Integer getDauByDate(String date);

        Integer getNewMidCountByDate(String date);

       List<DauPerHour> getDauPerHoursOfDay(String date);


       // gmv需求
    Double getGmvByDate(String date);

    List<GmvPerHour> getGmvPerHoursOfDay(String date);

    // 查询明细
    JSONObject getOrderDetailInfo(String date, Integer startpage , Integer size, String keyword) throws IOException;
}
