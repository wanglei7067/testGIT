package com.atguigu.gmall_publisher.dao;

import com.atguigu.gmall_publisher.beans.DauPerHour;
import com.atguigu.gmall_publisher.beans.GmvPerHour;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by VULCAN on 2021/5/29
 */
@Repository
public interface GMVMapper {

    Double getGmvByDate(String date);

    List<GmvPerHour> getGmvPerHoursOfDay(String date);

}
