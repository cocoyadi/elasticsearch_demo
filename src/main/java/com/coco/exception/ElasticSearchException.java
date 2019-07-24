
package com.coco.exception;

/**
 * @Description: 定义异常类
 * @author zhangxiaoxun
 * @date 2019/7/21 17:04
 * @Version: V1.0
 *
 **/
public class ElasticSearchException extends Exception{
    private static final long serialVersionUID = 1L;

    public ElasticSearchException() {
    }

    public ElasticSearchException(String message) {
        new ElasticSearchException(message, null);
    }

    public ElasticSearchException(String message, Exception e) {
        super(message, e);
    }
}
