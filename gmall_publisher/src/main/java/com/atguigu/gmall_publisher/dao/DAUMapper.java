package com.atguigu.gmall_publisher.dao;

import com.atguigu.gmall_publisher.beans.DauPerHour;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by VULCAN on 2021/5/28
 */
@Repository
public interface DAUMapper {

    Integer getDauByDate(String date);

    Integer getNewMidCountByDate(String date);

    List<DauPerHour> getDauPerHoursOfDay(String date);
}
