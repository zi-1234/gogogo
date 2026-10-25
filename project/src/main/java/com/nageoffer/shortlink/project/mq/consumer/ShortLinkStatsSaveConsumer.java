package com.nageoffer.shortlink.project.mq.consumer;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.Week;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.shortlink.project.common.convention.exception.ServiceException;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessLogsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkBrowserStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkDeviceStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkLocaleStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkNetworkStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkOsStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkStatsTodayDO;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkGotoDO;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessLogsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkBrowserStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkDeviceStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkLocaleStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkNetworkStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkOsStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkStatsTodayMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkGotoMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkMapper;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsBatchItem;
import com.nageoffer.shortlink.project.mq.idempotent.MessageQueueIdempotentHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_GID_UPDATE_KEY;
import static com.nageoffer.shortlink.project.common.constant.ShortLinkConstant.AMAP_REMOTE_URL;

/**
 * 短链接监控状态保存消息队列消费者
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = "${rocketmq.producer.topic}",
        consumerGroup = "${rocketmq.consumer.group}",
        maxReconsumeTimes = 16
)
public class ShortLinkStatsSaveConsumer implements RocketMQListener<Map<String, String>>, RocketMQPushConsumerLifecycleListener {
//public class ShortLinkStatsSaveConsumer implements RocketMQListener<Map<String, String>> {

    private final ShortLinkMapper shortLinkMapper;
    private final ShortLinkGotoMapper shortLinkGotoMapper;
    private final RedissonClient redissonClient;
    private final LinkAccessStatsMapper linkAccessStatsMapper;
    private final LinkLocaleStatsMapper linkLocaleStatsMapper;
    private final LinkOsStatsMapper linkOsStatsMapper;
    private final LinkBrowserStatsMapper linkBrowserStatsMapper;
    private final LinkAccessLogsMapper linkAccessLogsMapper;
    private final LinkDeviceStatsMapper linkDeviceStatsMapper;
    private final LinkNetworkStatsMapper linkNetworkStatsMapper;
    private final LinkStatsTodayMapper linkStatsTodayMapper;
    private final MessageQueueIdempotentHandler messageQueueIdempotentHandler;
    private final TransactionTemplate transactionTemplate;

    @Value("${short-link.stats.locale.amap-key}")
    private String statsLocaleAmapKey;

    @Value("${rocketmq.consumer.consume-message-batch-max-size:32}")
    private int consumeMessageBatchMaxSize;

    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        consumer.setConsumeMessageBatchMaxSize(Math.max(1, Math.min(consumeMessageBatchMaxSize, 100)));
        consumer.setMessageListener((MessageListenerConcurrently) this::consumeBatch);
    }

    private ConsumeConcurrentlyStatus consumeBatch(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
        String batchId = null;
        Set<String> pendingKeys = new LinkedHashSet<>();
        try {
            List<Map<String, String>> payloads = messages.stream()
                    .map(message -> JSON.parseObject(new String(message.getBody(), java.nio.charset.StandardCharsets.UTF_8), Map.class))
                    .map(payload -> (Map<String, String>) payload)
                    .toList();
            batchId = batchId(payloads);
            for (Map<String, String> payload : payloads) {
                String key = payload.get("keys");
                if (StrUtil.isBlank(key)) {
                    throw new ServiceException("统计消息缺少幂等标识");
                }
                if (messageQueueIdempotentHandler.isMessageProcessed(key)) {
                    if (!messageQueueIdempotentHandler.isAccomplish(key)) {
                        throw new ServiceException("统计消息仍在处理中");
                    }
                    continue;
                }
                pendingKeys.add(key);
            }
            if (pendingKeys.isEmpty()) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
            List<ShortLinkStatsBatchItem> items = payloads.stream()
                    .filter(payload -> pendingKeys.contains(payload.get("keys")))
                    .map(this::buildBatchItem)
                    .toList();
            transactionTemplate.executeWithoutResult(status -> batchSaveShortLinkStats(items));
            try {
                pendingKeys.forEach(messageQueueIdempotentHandler::setAccomplish);
            } catch (Exception ex) {
                log.error("redis幂等性标记设置完成时异常，数据库已落库");
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Throwable ex) {
            messages.forEach(message -> {
                try {
                    Map<String, String> payload = JSON.parseObject(new String(message.getBody(), java.nio.charset.StandardCharsets.UTF_8), Map.class);
                    String key = payload.get("keys");
                    if (key != null && pendingKeys.contains(key)) {
                        messageQueueIdempotentHandler.delMessageProcessed(key);
                    }
                } catch (Exception ignored) {
                    // 保留原始异常，交给 RocketMQ 重试。
                }
            });
            log.error("短链接统计批量消费失败，消息数：{}，批次：{}", messages.size(), batchId, ex);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    private ShortLinkStatsBatchItem buildBatchItem(Map<String, String> payload) {
        ShortLinkStatsRecordDTO record = JSON.parseObject(payload.get("statsRecord"), ShortLinkStatsRecordDTO.class);
        if (record == null || StrUtil.isBlank(record.getFullShortUrl())) {
            throw new ServiceException("统计消息缺少短链接");
        }
        ShortLinkGotoDO gotoDO = shortLinkGotoMapper.selectOne(Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                .eq(ShortLinkGotoDO::getFullShortUrl, record.getFullShortUrl()));
        if (gotoDO == null) {
            throw new ServiceException("短链接分组不存在");
        }
        Date sourceDate = record.getCurrentDate() == null ? new Date() : record.getCurrentDate();
        Date date = DateUtil.beginOfDay(sourceDate);
        String province = "未知";
        String city = "未知";
        String adcode = "未知";
        try {
            Map<String, Object> params = new HashMap<>();
            params.put("key", statsLocaleAmapKey);
            params.put("ip", record.getRemoteAddr());
            JSONObject locale = JSON.parseObject(HttpUtil.get(AMAP_REMOTE_URL, params));
            if (locale != null && StrUtil.equals(locale.getString("infocode"), "10000")) {
                province = StrUtil.blankToDefault(locale.getString("province"), province);
                city = StrUtil.blankToDefault(locale.getString("city"), city);
                adcode = StrUtil.blankToDefault(locale.getString("adcode"), adcode);
            }
        } catch (Exception ex) {
            log.warn("批量统计地区解析失败，降级为未知，短链接：{}", record.getFullShortUrl(), ex);
        }
        return ShortLinkStatsBatchItem.builder()
                .messageKey(payload.get("keys")).gid(gotoDO.getGid()).fullShortUrl(record.getFullShortUrl())
                .date(date).hour(DateUtil.hour(sourceDate, true)).weekday(DateUtil.dayOfWeekEnum(sourceDate).getIso8601Value())
                .pv(1).uv(Boolean.TRUE.equals(record.getUvFirstFlag()) ? 1 : 0)
                .uip(Boolean.TRUE.equals(record.getUipFirstFlag()) ? 1 : 0)
                .province(province).city(city).adcode(adcode).country("中国")
                .os(record.getOs()).browser(record.getBrowser()).device(record.getDevice()).network(record.getNetwork())
                .user(record.getUv()).ip(record.getRemoteAddr())
                .locale(StrUtil.join("-", "中国", province, city)).build();
    }

    @Transactional(rollbackFor = Exception.class)
    public void batchSaveShortLinkStats(List<ShortLinkStatsBatchItem> items) {
        if (items == null || items.isEmpty()) {
            throw new ServiceException("统计批次没有可写入的数据");
        }
        Map<String, ShortLinkStatsBatchItem> grouped = items.stream().collect(java.util.stream.Collectors.toMap(
                item -> item.getFullShortUrl() + "|" + item.getDate() + "|" + item.getHour() + "|" + item.getWeekday(),
                item -> item,
                (left, right) -> merge(left, right)));
        linkAccessStatsMapper.batchShortLinkStats(grouped.values().stream().map(item -> LinkAccessStatsDO.builder().fullShortUrl(item.getFullShortUrl()).date(item.getDate()).hour(item.getHour()).weekday(item.getWeekday()).pv(item.getPv()).uv(item.getUv()).uip(item.getUip()).build()).toList());
        linkLocaleStatsMapper.batchShortLinkLocaleStats(aggregateLocale(items));
        linkOsStatsMapper.batchShortLinkOsStats(aggregateOs(items));
        linkBrowserStatsMapper.batchShortLinkBrowserStats(aggregateBrowser(items));
        linkDeviceStatsMapper.batchShortLinkDeviceStats(aggregateDevice(items));
        linkNetworkStatsMapper.batchShortLinkNetworkStats(aggregateNetwork(items));
        linkStatsTodayMapper.batchShortLinkTodayStats(aggregateToday(items));
        shortLinkMapper.incrementStatsBatch(aggregateLink(items));
        linkAccessLogsMapper.batchInsert(items.stream().map(item -> LinkAccessLogsDO.builder().fullShortUrl(item.getFullShortUrl()).user(item.getUser()).ip(item.getIp()).browser(item.getBrowser()).os(item.getOs()).network(item.getNetwork()).device(item.getDevice()).locale(item.getLocale()).build()).toList());
    }

    private ShortLinkStatsBatchItem merge(ShortLinkStatsBatchItem left, ShortLinkStatsBatchItem right) {
        left.setPv(left.getPv() + right.getPv()); left.setUv(left.getUv() + right.getUv()); left.setUip(left.getUip() + right.getUip()); return left;
    }

    private List<LinkLocaleStatsDO> aggregateLocale(List<ShortLinkStatsBatchItem> items) { return items.stream().collect(java.util.stream.Collectors.toMap(i -> i.getFullShortUrl()+"|"+i.getDate()+"|"+i.getProvince()+"|"+i.getCity()+"|"+i.getAdcode(), i -> LinkLocaleStatsDO.builder().fullShortUrl(i.getFullShortUrl()).date(i.getDate()).country(i.getCountry()).province(i.getProvince()).city(i.getCity()).adcode(i.getAdcode()).cnt(1).build(), (a,b)->{a.setCnt(a.getCnt()+b.getCnt()); return a;})).values().stream().toList(); }
    private List<LinkOsStatsDO> aggregateOs(List<ShortLinkStatsBatchItem> items) { return items.stream().collect(java.util.stream.Collectors.toMap(i -> i.getFullShortUrl()+"|"+i.getDate()+"|"+i.getOs(), i -> LinkOsStatsDO.builder().fullShortUrl(i.getFullShortUrl()).date(i.getDate()).os(i.getOs()).cnt(1).build(), (a,b)->{a.setCnt(a.getCnt()+b.getCnt()); return a;})).values().stream().toList(); }
    private List<LinkBrowserStatsDO> aggregateBrowser(List<ShortLinkStatsBatchItem> items) { return items.stream().collect(java.util.stream.Collectors.toMap(i -> i.getFullShortUrl()+"|"+i.getDate()+"|"+i.getBrowser(), i -> LinkBrowserStatsDO.builder().fullShortUrl(i.getFullShortUrl()).date(i.getDate()).browser(i.getBrowser()).cnt(1).build(), (a,b)->{a.setCnt(a.getCnt()+b.getCnt()); return a;})).values().stream().toList(); }
    private List<LinkDeviceStatsDO> aggregateDevice(List<ShortLinkStatsBatchItem> items) { return items.stream().collect(java.util.stream.Collectors.toMap(i -> i.getFullShortUrl()+"|"+i.getDate()+"|"+i.getDevice(), i -> LinkDeviceStatsDO.builder().fullShortUrl(i.getFullShortUrl()).date(i.getDate()).device(i.getDevice()).cnt(1).build(), (a,b)->{a.setCnt(a.getCnt()+b.getCnt()); return a;})).values().stream().toList(); }
    private List<LinkNetworkStatsDO> aggregateNetwork(List<ShortLinkStatsBatchItem> items) { return items.stream().collect(java.util.stream.Collectors.toMap(i -> i.getFullShortUrl()+"|"+i.getDate()+"|"+i.getNetwork(), i -> LinkNetworkStatsDO.builder().fullShortUrl(i.getFullShortUrl()).date(i.getDate()).network(i.getNetwork()).cnt(1).build(), (a,b)->{a.setCnt(a.getCnt()+b.getCnt()); return a;})).values().stream().toList(); }
    private List<LinkStatsTodayDO> aggregateToday(List<ShortLinkStatsBatchItem> items) { return items.stream().collect(java.util.stream.Collectors.toMap(i -> i.getFullShortUrl()+"|"+i.getDate(), i -> LinkStatsTodayDO.builder().fullShortUrl(i.getFullShortUrl()).date(i.getDate()).todayPv(1).todayUv(i.getUv()).todayUip(i.getUip()).build(), (a,b)->{a.setTodayPv(a.getTodayPv()+b.getTodayPv()); a.setTodayUv(a.getTodayUv()+b.getTodayUv()); a.setTodayUip(a.getTodayUip()+b.getTodayUip()); return a;})).values().stream().toList(); }
    private List<ShortLinkStatsBatchItem> aggregateLink(List<ShortLinkStatsBatchItem> items) { return items.stream().collect(java.util.stream.Collectors.toMap(i -> i.getGid()+"|"+i.getFullShortUrl(), i -> ShortLinkStatsBatchItem.builder().gid(i.getGid()).fullShortUrl(i.getFullShortUrl()).pv(1).uv(i.getUv()).uip(i.getUip()).build(), (a,b)->merge(a,b))).values().stream().toList(); }

    private String batchId(List<Map<String, String>> payloads) {
        String keys = payloads.stream().map(payload -> payload.get("keys")).sorted().collect(java.util.stream.Collectors.joining("|"));
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(keys.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                result.append(String.format("%02x", value));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 不可用", ex);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void onMessage(Map<String, String> producerMap) {
        String keys = producerMap.get("keys");
        if (StrUtil.isBlank(keys)) {
            throw new ServiceException("统计消息缺少幂等标识");
        }
        if (messageQueueIdempotentHandler.isMessageProcessed(keys)) {
            // 判断当前的这个消息流程是否执行完成
            if (messageQueueIdempotentHandler.isAccomplish(keys)) {
                return;
            }
            throw new ServiceException("消息未完成流程，需要消息队列重试");
        }
        try {
            ShortLinkStatsRecordDTO statsRecord = JSON.parseObject(producerMap.get("statsRecord"), ShortLinkStatsRecordDTO.class);
            String fullShortUrl = statsRecord.getFullShortUrl();
            if (StrUtil.isNotBlank(fullShortUrl)) {
                actualSaveShortLinkStats(statsRecord);
            }
        } catch (Throwable ex) {
            // 删除幂等标识
            messageQueueIdempotentHandler.delMessageProcessed(keys);
            log.error("记录短链接监控消费异常", ex);
            throw ex;
        }
        messageQueueIdempotentHandler.setAccomplish(keys);
    }

    public void actualSaveShortLinkStats(ShortLinkStatsRecordDTO statsRecord) {
        String fullShortUrl = statsRecord.getFullShortUrl();
        RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(String.format(LOCK_GID_UPDATE_KEY, fullShortUrl));
        RLock rLock = readWriteLock.readLock();
        rLock.lock();
        try {
            LambdaQueryWrapper<ShortLinkGotoDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                    .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl);
            ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(queryWrapper);
            String gid = shortLinkGotoDO.getGid();
            int hour = DateUtil.hour(new Date(), true);
            Week week = DateUtil.dayOfWeekEnum(new Date());
            int weekValue = week.getIso8601Value();
            LinkAccessStatsDO linkAccessStatsDO = LinkAccessStatsDO.builder()
                    .pv(1)
                    .uv(statsRecord.getUvFirstFlag() ? 1 : 0)
                    .uip(statsRecord.getUipFirstFlag() ? 1 : 0)
                    .hour(hour)
                    .weekday(weekValue)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkAccessStatsMapper.shortLinkStats(linkAccessStatsDO);
            String actualProvince = "未知";
            String actualCity = "未知";
            try {
                Map<String, Object> localeParamMap = new HashMap<>();
                localeParamMap.put("key", statsLocaleAmapKey);
                localeParamMap.put("ip", statsRecord.getRemoteAddr());
                String localeResultStr = HttpUtil.get(AMAP_REMOTE_URL, localeParamMap);
                JSONObject localeResultObj = JSON.parseObject(localeResultStr);
                String infoCode = localeResultObj.getString("infocode");
                if (StrUtil.isNotBlank(infoCode) && StrUtil.equals(infoCode, "10000")) {
                    String province = localeResultObj.getString("province");
                    boolean unknownFlag = StrUtil.equals(province, "[]");
                    actualProvince = unknownFlag ? actualProvince : StrUtil.blankToDefault(province, actualProvince);
                    actualCity = unknownFlag ? actualCity : StrUtil.blankToDefault(localeResultObj.getString("city"), actualCity);
                    LinkLocaleStatsDO linkLocaleStatsDO = LinkLocaleStatsDO.builder()
                            .province(actualProvince)
                            .city(actualCity)
                            .adcode(unknownFlag ? "未知" : StrUtil.blankToDefault(localeResultObj.getString("adcode"), "未知"))
                            .cnt(1)
                            .fullShortUrl(fullShortUrl)
                            .country("中国")
                            .date(new Date())
                            .build();
                    linkLocaleStatsMapper.shortLinkLocaleState(linkLocaleStatsDO);
                }
            } catch (Exception localeException) {
                log.warn("高德地区解析失败，降级为未知地区，短链接：{}", fullShortUrl, localeException);
            }
            LinkOsStatsDO linkOsStatsDO = LinkOsStatsDO.builder()
                    .os(statsRecord.getOs())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkOsStatsMapper.shortLinkOsState(linkOsStatsDO);
            LinkBrowserStatsDO linkBrowserStatsDO = LinkBrowserStatsDO.builder()
                    .browser(statsRecord.getBrowser())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkBrowserStatsMapper.shortLinkBrowserState(linkBrowserStatsDO);
            LinkDeviceStatsDO linkDeviceStatsDO = LinkDeviceStatsDO.builder()
                    .device(statsRecord.getDevice())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkDeviceStatsMapper.shortLinkDeviceState(linkDeviceStatsDO);
            LinkNetworkStatsDO linkNetworkStatsDO = LinkNetworkStatsDO.builder()
                    .network(statsRecord.getNetwork())
                    .cnt(1)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkNetworkStatsMapper.shortLinkNetworkState(linkNetworkStatsDO);
            LinkAccessLogsDO linkAccessLogsDO = LinkAccessLogsDO.builder()
                    .user(statsRecord.getUv())
                    .ip(statsRecord.getRemoteAddr())
                    .browser(statsRecord.getBrowser())
                    .os(statsRecord.getOs())
                    .network(statsRecord.getNetwork())
                    .device(statsRecord.getDevice())
                    .locale(StrUtil.join("-", "中国", actualProvince, actualCity))
                    .fullShortUrl(fullShortUrl)
                    .build();
            linkAccessLogsMapper.insert(linkAccessLogsDO);
            shortLinkMapper.incrementStats(gid, fullShortUrl, 1, statsRecord.getUvFirstFlag() ? 1 : 0, statsRecord.getUipFirstFlag() ? 1 : 0);
            LinkStatsTodayDO linkStatsTodayDO = LinkStatsTodayDO.builder()
                    .todayPv(1)
                    .todayUv(statsRecord.getUvFirstFlag() ? 1 : 0)
                    .todayUip(statsRecord.getUipFirstFlag() ? 1 : 0)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkStatsTodayMapper.shortLinkTodayState(linkStatsTodayDO);
        } finally {
            rLock.unlock();
        }
    }
}
