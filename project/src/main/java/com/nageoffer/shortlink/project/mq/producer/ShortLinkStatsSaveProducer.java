package com.nageoffer.shortlink.project.mq.producer;

import com.alibaba.fastjson2.JSON;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 短链接监控状态保存消息队列生产者
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShortLinkStatsSaveProducer {

    private final RocketMQTemplate rocketMQTemplate;

    @Value("${rocketmq.producer.topic}")
    private String statsSaveTopic;

    @Value("${rocketmq.producer.retry-times-when-send-async-failed:1}")
    private int maxAsyncRetryTimes;

    /**
     * 发送延迟消费短链接统计
     */
    public void send(Map<String, String> producerMap) {
        String keys = UUID.randomUUID().toString();
        producerMap.put("keys", keys);
        Message<Map<String, String>> build = MessageBuilder
                .withPayload(producerMap)
                .setHeader(MessageConst.PROPERTY_KEYS, keys)
                .build();
        AtomicInteger retryCounter = new AtomicInteger();
        sendAsync(build, producerMap, keys, retryCounter);
    }

    private void sendAsync(Message<Map<String, String>> message, Map<String, String> producerMap,
                           String keys, AtomicInteger retryCounter) {
        try {
            rocketMQTemplate.asyncSend(statsSaveTopic, message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("[消息访问统计监控] 消息发送结果：{}，消息ID：{}，消息Keys：{}", sendResult.getSendStatus(), sendResult.getMsgId(), keys);
                }

                @Override
                public void onException(Throwable ex) {
                    int retry = retryCounter.incrementAndGet();
                    if (retry <= maxAsyncRetryTimes) {
                        log.warn("[消息访问统计监控] 消息发送失败，准备第 {} 次重试，消息Keys：{}", retry, keys, ex);
                        sendAsync(message, producerMap, keys, retryCounter);
                        return;
                    }
                    log.error("[消息访问统计监控] 消息发送失败且重试耗尽，消息体：{}，消息Keys：{}",
                            JSON.toJSONString(producerMap), keys, ex);
                }
            }, 2000L);
        } catch (Throwable ex) {
            int retry = retryCounter.incrementAndGet();
            if (retry <= maxAsyncRetryTimes) {
                log.warn("[消息访问统计监控] 异步发送调用异常，准备第 {} 次重试，消息Keys：{}", retry, keys, ex);
                sendAsync(message, producerMap, keys, retryCounter);
                return;
            }
            log.error("[消息访问统计监控] 异步发送调用异常且重试耗尽，消息体：{}，消息Keys：{}",
                    JSON.toJSONString(producerMap), keys, ex);
        }
    }
}

