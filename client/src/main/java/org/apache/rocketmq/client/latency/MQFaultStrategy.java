/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息失败策略，延迟实现的门面类
 */
public class MQFaultStrategy {

    private final static InternalLogger log = ClientLogger.getLog();

    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    //是否启用MQ故障延迟机制
    private boolean sendLatencyFaultEnable = false;

    /**
     * 根据 currentLatency 本次消息发送延迟，从 latencyMax 尾部向前找到第一个比 currentLatency 小的索引index，
     * 如果没有找到，返回 0。
     * 然后根据这个索引从 notAvailableDuration 数组中取出对应的事件，在这个时间长内，Broker将设置为不可用
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};

    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 从 TopicPublishInfo 选择一个 MessageQueue消息队列
     * @param tpInfo
     * @param lastBrokerName 上一次执行发送消息失败的 Broker。 第一次发送的时候，为null
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //选择队列的Broker故障延迟机制
        if (this.sendLatencyFaultEnable) {
            try {
                //选择队列次数增加 1
                int index = tpInfo.getSendWhichQueue().getAndIncrement();

                //1. 首先对消息队列进行轮询获取一个消息队列
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;

                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //2. 验证队列是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                //尝试从规避中的Broker 中选择一个可用Broker，如果没有找到，将返回null
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    //?????如果返回的MessageQueue 可用，移除latencyFaultTolerance 关于该Topic 条目，表明该Broker 故障已恢复
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        //默认选择队列机制
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 更新消息发送状态
     *
     * @param brokerName broker名称
     * @param currentLatency 本次消息发送延迟时间currentLatency
     * @param isolation 是否隔离。
     *                  如果为true，则使用默认时长 30s 来计算 Broker 故障规避时长；
     *                  如果为false，则使用本次消息发送延迟时间来计算Broker 故障规避时长。
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            //根据消息发送延迟时间，计算 broker 不可用时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            //更新失败条目
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
