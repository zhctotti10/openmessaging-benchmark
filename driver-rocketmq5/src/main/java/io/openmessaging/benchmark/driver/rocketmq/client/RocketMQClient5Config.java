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
package io.openmessaging.benchmark.driver.rocketmq.client;

public class RocketMQClient5Config {
    public String namesrvAddr;
    public String proxyEndpoint;
    public String clusterName;
    public String accessKey;
    public String secretKey;
    public boolean useMyTopic;
    public String topics;
    public boolean useCustomNamespace;
    public String customNamespace;
    public boolean sendDelayMsg;
    public Long delayTimeInSec;
}
