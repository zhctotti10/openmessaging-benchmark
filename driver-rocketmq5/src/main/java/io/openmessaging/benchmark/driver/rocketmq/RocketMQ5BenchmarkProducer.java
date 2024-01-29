/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.rocketmq;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;

public class RocketMQ5BenchmarkProducer implements BenchmarkProducer {
    private final Producer rmqProducer;
    private final String rmqTopic;
    private final Boolean sendDelayMsg;
    private final Long delayTimeInSec;

    final ClientServiceProvider provider = ClientServiceProvider.loadService();

    public RocketMQ5BenchmarkProducer(final Producer rmqProducer, final String rmqTopic) {
        this.rmqProducer = rmqProducer;
        this.rmqTopic = rmqTopic;
        this.sendDelayMsg = false;
        this.delayTimeInSec = 0L;
    }

    public RocketMQ5BenchmarkProducer(final Producer rmqProducer, final String rmqTopic, Boolean sendDelayMsg,
        Long delayTimeInSec) {
        this.rmqProducer = rmqProducer;
        this.rmqTopic = rmqTopic;
        this.sendDelayMsg = sendDelayMsg;
        this.delayTimeInSec = delayTimeInSec;
    }

    @Override
    public CompletableFuture<Void> sendAsync(final Optional<String> key, final byte[] payload) {
        MessageBuilder messageBuilder = provider.newMessageBuilder()
            // Set topic for the current message.
            .setTopic(this.rmqTopic)
            // Message secondary classifier of message besides topic.
            // Key(s) of the message, another way to mark message besides message id.
            .setBody(payload);

        if (key.isPresent()) {
            messageBuilder.setKeys(key.get());
        }

        if (this.sendDelayMsg) {
            // 延时消息，单位秒（s），在指定延迟时间（当前时间之后）进行投递，例如消息在10秒后投递。
            long delayTime = System.currentTimeMillis() + this.delayTimeInSec * 1000;
            // 设置消息需要被投递的时间。
            messageBuilder.setDeliveryTimestamp(delayTime);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            SendReceipt result = this.rmqProducer.send(messageBuilder.build());
            if (result != null) {
                future.complete(null);
            } else {
                future.completeExceptionally(new RuntimeException("send failed"));
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public void close() throws Exception {
        // Close in Driver
    }
}
