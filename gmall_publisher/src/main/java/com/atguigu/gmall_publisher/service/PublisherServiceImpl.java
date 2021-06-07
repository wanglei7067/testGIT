package com.atguigu.gmall_publisher.service;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_publisher.beans.DauPerHour;
import com.atguigu.gmall_publisher.beans.GmvPerHour;
import com.atguigu.gmall_publisher.dao.DAUMapper;
import com.atguigu.gmall_publisher.dao.GMVMapper;
import com.atguigu.gmall_publisher.esdao.ESDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * Created by VULCAN on 2021/5/28
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DAUMapper dauMapper;

    @Autowired
    private GMVMapper gmvMapper;

    @Autowired
    private ESDao esDao;



    @Override
    public Integer getDauByDate(String date) {

        //业务逻辑

        return dauMapper.getDauByDate(date);
    }

    @Override
    public Integer getNewMidCountByDate(String date) {
        return dauMapper.getNewMidCountByDate(date);
    }

    @Override
    public List<DauPerHour> getDauPerHoursOfDay(String date) {
        return dauMapper.getDauPerHoursOfDay(date);
    }

    @Override
    public Double getGmvByDate(String date) {
        return gmvMapper.getGmvByDate(date);
    }

    @Override
    public List<GmvPerHour> getGmvPerHoursOfDay(String date) {
        return gmvMapper.getGmvPerHoursOfDay(date);
    }

    @Override
    public JSONObject getOrderDetailInfo(String date, Integer startpage, Integer size, String keyword) throws IOException {
        return esDao.getOrderDetail(date, startpage, size, keyword);
    }
}
