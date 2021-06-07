package com.atguigu.gmall_publisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by VULCAN on 2021/5/28
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GmvPerHour {

    private String hour;
    private Double gmv;
}
