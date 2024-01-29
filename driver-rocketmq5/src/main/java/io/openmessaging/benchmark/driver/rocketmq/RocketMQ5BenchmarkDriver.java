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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Splitter;
import com.google.common.io.BaseEncoding;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.rocketmq.client.RocketMQClient5Config;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMQ5BenchmarkDriver implements BenchmarkDriver {
    private DefaultMQAdminExt rmqAdmin;
    private RocketMQClient5Config rmqClientConfig;
    Producer rmqProducer;
    private RPCHook rpcHook;

    @Override
    public void initialize(final File configurationFile, final StatsLogger statsLogger) throws IOException {
        this.rmqClientConfig = readConfig(configurationFile);
        if (isAclEnabled()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey));
            this.rmqAdmin = new DefaultMQAdminExt(rpcHook);
        } else {
            this.rmqAdmin = new DefaultMQAdminExt();
        }
        this.rmqAdmin.setNamesrvAddr(this.rmqClientConfig.namesrvAddr);
        this.rmqAdmin.setInstanceName("AdminInstance-" + getRandomString());
        try {
            this.rmqAdmin.start();
        } catch (MQClientException e) {
            log.error("Start the RocketMQ5 admin tool failed.");
        }

    }

    @Override
    public String getTopicNamePrefix() {
        return "RocketMQ-Benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(final String topic, final int partitions) {
        return CompletableFuture.runAsync(() -> {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setOrder(false);
            topicConfig.setPerm(6);
            topicConfig.setReadQueueNums(partitions);
            topicConfig.setWriteQueueNums(partitions);
            topicConfig.setTopicName(topic);

            try {
                Set<String> brokerList = CommandUtil.fetchMasterAddrByClusterName(this.rmqAdmin, this.rmqClientConfig.clusterName);
                topicConfig.setReadQueueNums(Math.max(1, partitions / brokerList.size()));
                topicConfig.setWriteQueueNums(Math.max(1, partitions / brokerList.size()));

                for (String brokerAddr : brokerList) {
                    this.rmqAdmin.createAndUpdateTopicConfig(brokerAddr, topicConfig);
                }
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to create topic [%s] to cluster [%s]", topic, this.rmqClientConfig.clusterName), e);
            }
        });
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(final String topic) {
        String newTopic = topic;
        if (rmqProducer == null) {
            ClientServiceProvider provider = ClientServiceProvider.loadService();
            ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(this.rmqClientConfig.proxyEndpoint);
            SessionCredentialsProvider sessionCredentialsProvider =
                new StaticSessionCredentialsProvider(this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey);
            ClientConfiguration configuration;
            if (isAclEnabled()) {
                builder.setCredentialProvider(sessionCredentialsProvider);
                configuration = builder.build();
            } else {
                configuration = builder.build();
            }

            if (rmqClientConfig.useCustomNamespace) {
                newTopic = rmqClientConfig.customNamespace + "%" + topic;
            }

            try {
                rmqProducer = provider.newProducerBuilder()
                    .setTopics(newTopic)
                    .setClientConfiguration(configuration)
                    .build();
            } catch (ClientException e) {
                throw new RuntimeException(e);
            }
        }

        if (this.rmqClientConfig.sendDelayMsg) {
            return CompletableFuture.completedFuture(new RocketMQ5BenchmarkProducer(rmqProducer, newTopic, true,
                this.rmqClientConfig.delayTimeInSec));
        } else {
            return CompletableFuture.completedFuture(new RocketMQ5BenchmarkProducer(rmqProducer, newTopic));
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(final String topic, final String subscriptionName,
        final ConsumerCallback consumerCallback) {
        PushConsumer rmqConsumer;

        // To avoid bench-tool encounter subscription relationship conflict when specifying multiple topics, let's add topic name as subscription name prefix.
        String subPrefix;
        if (topic.contains("%")) {
            subPrefix = topic.split("%")[1];
        } else {
            subPrefix = topic;
        }
        String fullSubName = String.format("%s_%s", subPrefix, subscriptionName);

        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(this.rmqClientConfig.proxyEndpoint);
        SessionCredentialsProvider sessionCredentialsProvider =
            new StaticSessionCredentialsProvider(this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey);
        ClientConfiguration configuration;
        if (isAclEnabled()) {
            builder.setCredentialProvider(sessionCredentialsProvider);
            configuration = builder.build();
        } else {
            configuration = builder.build();
        }
        FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);

        try {
            rmqConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(configuration)
                // Set the consumer group name.
                .setConsumerGroup(fullSubName)
                // Set the subscription for the consumer.
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .setMessageListener(messageView -> {
                    // Handle the received message and return consume result.
                    consumerCallback.messageReceived(messageView.getBody(), messageView.getBornTimestamp());
                    return ConsumeResult.SUCCESS;
                })
                .build();
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }
        return CompletableFuture.completedFuture(new RocketMQ5BenchmarkConsumer(rmqConsumer));
    }

    public boolean isAclEnabled() {
        return !(StringUtils.isAnyBlank(this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey) ||
            StringUtils.isAnyEmpty(this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey));
    }

    @Override
    public void close() throws Exception {
        if (this.rmqProducer != null) {
            this.rmqProducer.close();
        }
        this.rmqAdmin.shutdown();
    }

    @Override
    public boolean useMyTopic() {
        return rmqClientConfig.useMyTopic;
    }

    @Override
    public List<String> myTopic() {
        String originalTopic = rmqClientConfig.topics;
        return Splitter.on(",").splitToList(originalTopic);
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static RocketMQClient5Config readConfig(File configurationFile) throws IOException {
        return mapper.readValue(configurationFile, RocketMQClient5Config.class);
    }

    private static final Random random = new Random();

    private static final String getRandomString() {
        byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        return BaseEncoding.base64Url().omitPadding().encode(buffer);
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
    private static final Logger log = LoggerFactory.getLogger(RocketMQ5BenchmarkDriver.class);
}
