package com.atguigu.gmall_publisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class Stat {
    String title;
    List<Option> options;
}

