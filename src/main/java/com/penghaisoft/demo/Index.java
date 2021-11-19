package com.penghaisoft.demo;

import lombok.Builder;
import lombok.Data;

/**
 * @Author Zhengwenxin
 * @Date 2021/11/11 9:18
 * @Version 1.0
 * @Description
 */
@Data
@Builder
public class Index {
    private String type;
    private String index;
    private String id;
}
