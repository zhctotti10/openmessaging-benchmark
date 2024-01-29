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

package io.openmessaging.benchmark.driver.pulsar;

import com.google.common.base.Splitter;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.pulsar.config.PulsarConfig;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.shade.org.glassfish.jersey.internal.guava.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarBenchmarkProducer implements BenchmarkProducer {

    private final Producer<byte[]> producer;
    private final List<String> tags;
    private final List<String> keys;
    private final List<DelayTime> delayTimes;
    private final Random random;

    @Override
    public void close() throws Exception {
        try{
            producer.close();
        } catch (PulsarClientException.AlreadyClosedException e) {
            // ignore
            log.warn("Ignore AlreadyClosedException:\n" + e.getMessage());
        }
    }

    public PulsarBenchmarkProducer(Producer<byte[]> producer, PulsarConfig config) {
        this.producer = producer;
        this.tags = Lists
                .newArrayList(Splitter.on(",").trimResults().omitEmptyStrings().splitToList(config.producer.tags));
        if (this.tags.isEmpty() && config.producer.tagNum > 0) {
            for (int i = 0; i < config.producer.tagNum; i++) {
                tags.add("tag-" + i);
            }
        }
        this.keys = Lists
                .newArrayList(Splitter.on(",").trimResults().omitEmptyStrings().splitToList(config.producer.keys));
        if (this.keys.isEmpty() && config.producer.keyNum > 0) {
            for (int i = 0; i < config.producer.keyNum; i++) {
                keys.add("key-" + UUID.randomUUID());
            }
        }

        this.delayTimes = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(config.producer.delayTimes)
                .stream().map(value -> {
                    int index = 0;
                    while (index < value.length()) {
                        if (!Character.isDigit(value.charAt(index))) {
                            break;
                        }
                        index++;
                    }
                    long delay = Long.parseLong(value.substring(0, index).trim());
                    String unit = value.substring(index).toLowerCase();
                    switch (unit) {
                        case "ms":
                        case "millisecond":
                            return new DelayTime(delay, TimeUnit.MILLISECONDS);
                        case "s":
                        case "second":
                            return new DelayTime(delay, TimeUnit.SECONDS);
                        case "m":
                        case "minute":
                            return new DelayTime(delay, TimeUnit.MINUTES);
                        case "h":
                        case "hour":
                            return new DelayTime(delay, TimeUnit.HOURS);
                        case "d":
                        case "day":
                            return new DelayTime(delay, TimeUnit.DAYS);
                        default:
                            throw new RuntimeException("Producer config delayTimes has invalid unit: " + unit);
                    }
                }).collect(Collectors.toList());
        this.random = new Random();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        TypedMessageBuilder<byte[]> msgBuilder = producer.newMessage().value(payload);
        key.ifPresent(msgBuilder::key);

        // support send tag message
        if (!tags.isEmpty()) {
            msgBuilder.property(tags.get(random.nextInt(tags.size()) % tags.size()), "TAGS");
        }

        // support send key message
        if (!keys.isEmpty()) {
            int idx = random.nextInt(keys.size()) % keys.size();
            msgBuilder.key(keys.get(idx));
        }

        // support send delay message
        if (!delayTimes.isEmpty()) {
            DelayTime delayTime = delayTimes.get(random.nextInt(delayTimes.size()) % delayTimes.size());
            msgBuilder.deliverAfter(delayTime.delay, delayTime.unit);
        }

        return msgBuilder.sendAsync().thenApply(msgId -> null);
    }

    private static class DelayTime {

        private final long delay;
        private final TimeUnit unit;

        public DelayTime(long delay, TimeUnit unit) {
            this.delay = delay;
            this.unit = unit;
        }

        @Override
        public String toString() {
            return delay + ":" + unit;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarBenchmarkProducer.class);
}
