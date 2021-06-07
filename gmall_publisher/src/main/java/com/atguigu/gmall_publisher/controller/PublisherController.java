package com.atguigu.gmall_publisher.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_publisher.beans.DauPerHour;
import com.atguigu.gmall_publisher.beans.GmvPerHour;
import com.atguigu.gmall_publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by VULCAN on 2021/5/28
 */
@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    /*
        http://localhost:8070/realtime-total  ?  date=2020-08-15

        [
        {"id":"dau","name":"新增日活","value":1200},  //当日日活
{"id":"new_mid","name":"新增设备","value":233}  ,    //当日新增
{"id":"order_amount","name":"新增交易额","value":1000.2 }
  ]

            List,Array,JSONArray
     */
    @RequestMapping(value = "/realtime-total")
    public Object handle1(String date){
        Integer dauByDate = publisherService.getDauByDate(date);
        Integer newMidCountByDate = publisherService.getNewMidCountByDate(date);
        Double gmvByDate = publisherService.getGmvByDate(date);

        ArrayList<JSONObject> result = new ArrayList<>();

        JSONArray jsonArray = new JSONArray();

        JSONObject jsonObject1 = new JSONObject();
        JSONObject jsonObject2 = new JSONObject();
        JSONObject jsonObject3 = new JSONObject();

        jsonObject1.put("id","dau");
        jsonObject1.put("name","新增日活");
        jsonObject1.put("value",dauByDate);

        jsonObject2.put("id","new_mid");
        jsonObject2.put("name","新增设备");
        jsonObject2.put("value",newMidCountByDate);

        jsonObject3.put("id","order_amount");
        jsonObject3.put("name","新增交易额");
        jsonObject3.put("value",gmvByDate);

        result.add(jsonObject1);
        result.add(jsonObject2);
        result.add(jsonObject3);

        jsonArray.add(jsonObject1.toJSONString());
        jsonArray.add(jsonObject2.toJSONString());

        // springboot判断是否是字面量，如果不是，利用jackson转为jsonstr
        return result;

    }

    /*
        http://localhost:8070/realtime-hours  ?id=dau&date=2020-08-15

        {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
"today":{"12":38,"13":1233,"17":123,"19":688 }}

            Map,JSONObject



            http://localhost:8070/realtime-hours?id=order_amount&date=2020-08-18
            {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
"today":{"12":38,"13":1233,"17":123,"19":688 }}
     */
    @RequestMapping(value = "/realtime-hours")
    public Object handle2(String date,String id){

        //求今天和昨天的日期
        LocalDate today = LocalDate.parse(date);
        LocalDate yestoday = today.minusDays(1);

        HashMap<String, JSONObject> result = new HashMap<>();

        if (id.equals("dau")){
            //求今天和昨天的分时数据
            List<DauPerHour> todayData = publisherService.getDauPerHoursOfDay(today.toString());
            List<DauPerHour> yestodayData = publisherService.getDauPerHoursOfDay(yestoday.toString());

            result.put("today",parseDauPerHour(todayData));
            result.put("yesterday",parseDauPerHour(yestodayData));


        }else{
            List<GmvPerHour> todayData = publisherService.getGmvPerHoursOfDay(today.toString());
            List<GmvPerHour> yestodayData = publisherService.getGmvPerHoursOfDay(yestoday.toString());

            result.put("today",parseGmvPerHour(todayData));
            result.put("yesterday",parseGmvPerHour(yestodayData));
        }

        return  result;


    }

    public JSONObject parseDauPerHour(List<DauPerHour>  datas){

        JSONObject jsonObject = new JSONObject();

        for (DauPerHour data : datas) {

            jsonObject.put(data.getHour(),data.getNums());

        }

        return jsonObject;

    }

    public JSONObject parseGmvPerHour(List<GmvPerHour>  datas){

        JSONObject jsonObject = new JSONObject();

        for (GmvPerHour data : datas) {

            jsonObject.put(data.getHour(),data.getGmv());

        }

        return jsonObject;

    }


}
