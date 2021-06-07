package com.dnow.gmall.dwlogger.cotroller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping(value = "/log")
    public Object handle(@RequestParam("logStr") String logStr){
        // 0 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logStr);
        jsonObject.put("ts",System.currentTimeMillis()/1000);

        // 1 落盘 file
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);

        // 2 推送到kafka
        if ("event".equals(jsonObject.get("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }

        return "success";
    }

}
