/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.LeaderAndIsrRequestData;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrLiveLeader;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrTopicState;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.FlattenedIterator;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
<<<<<<< HEAD
import java.util.Set;

public class LeaderAndIsrRequest extends AbstractRequest {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.LEADER_AND_ISR.id);

    private static final String CONTROLLER_ID_KEY_NAME = "controller_id";
    private static final String CONTROLLER_EPOCH_KEY_NAME = "controller_epoch";
    private static final String PARTITION_STATES_KEY_NAME = "partition_states";
    private static final String LIVE_LEADERS_KEY_NAME = "live_leaders";

    // partition_states key names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String LEADER_KEY_NAME = "leader";
    private static final String LEADER_EPOCH_KEY_NAME = "leader_epoch";
    private static final String ISR_KEY_NAME = "isr";
    private static final String ZK_VERSION_KEY_NAME = "zk_version";
    private static final String REPLICAS_KEY_NAME = "replicas";

    // live_leaders key names
    private static final String END_POINT_ID_KEY_NAME = "id";
    private static final String HOST_KEY_NAME = "host";
    private static final String PORT_KEY_NAME = "port";

    private final int controllerId;
    private final int controllerEpoch;
    private final Map<TopicPartition, PartitionState> partitionStates;
    private final Set<Node> liveLeaders;

    public LeaderAndIsrRequest(int controllerId, int controllerEpoch, Map<TopicPartition, PartitionState> partitionStates,
                               Set<Node> liveLeaders) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(CONTROLLER_ID_KEY_NAME, controllerId);
        struct.set(CONTROLLER_EPOCH_KEY_NAME, controllerEpoch);

        List<Struct> partitionStatesData = new ArrayList<>(partitionStates.size());
        for (Map.Entry<TopicPartition, PartitionState> entry : partitionStates.entrySet()) {
            Struct partitionStateData = struct.instance(PARTITION_STATES_KEY_NAME);
            TopicPartition topicPartition = entry.getKey();
            partitionStateData.set(TOPIC_KEY_NAME, topicPartition.topic());
            partitionStateData.set(PARTITION_KEY_NAME, topicPartition.partition());
            PartitionState partitionState = entry.getValue();
            partitionStateData.set(CONTROLLER_EPOCH_KEY_NAME, partitionState.controllerEpoch);
            partitionStateData.set(LEADER_KEY_NAME, partitionState.leader);
            partitionStateData.set(LEADER_EPOCH_KEY_NAME, partitionState.leaderEpoch);
            partitionStateData.set(ISR_KEY_NAME, partitionState.isr.toArray());
            partitionStateData.set(ZK_VERSION_KEY_NAME, partitionState.zkVersion);
            partitionStateData.set(REPLICAS_KEY_NAME, partitionState.replicas.toArray());
            partitionStatesData.add(partitionStateData);
        }
        struct.set(PARTITION_STATES_KEY_NAME, partitionStatesData.toArray());

        List<Struct> leadersData = new ArrayList<>(liveLeaders.size());
        for (Node leader : liveLeaders) {
            Struct leaderData = struct.instance(LIVE_LEADERS_KEY_NAME);
            leaderData.set(END_POINT_ID_KEY_NAME, leader.id());
            leaderData.set(HOST_KEY_NAME, leader.host());
            leaderData.set(PORT_KEY_NAME, leader.port());
            leadersData.add(leaderData);
        }
        struct.set(LIVE_LEADERS_KEY_NAME, leadersData.toArray());
=======
import java.util.stream.Collectors;

public class LeaderAndIsrRequest extends AbstractControlRequest {

    public static class Builder extends AbstractControlRequest.Builder<LeaderAndIsrRequest> {
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b

        private final List<LeaderAndIsrPartitionState> partitionStates;
        private final Collection<Node> liveLeaders;

        public Builder(short version, int controllerId, int controllerEpoch, long brokerEpoch,
                       List<LeaderAndIsrPartitionState> partitionStates, Collection<Node> liveLeaders) {
            super(ApiKeys.LEADER_AND_ISR, version, controllerId, controllerEpoch, brokerEpoch);
            this.partitionStates = partitionStates;
            this.liveLeaders = liveLeaders;
        }

        @Override
        public LeaderAndIsrRequest build(short version) {
            List<LeaderAndIsrLiveLeader> leaders = liveLeaders.stream().map(n -> new LeaderAndIsrLiveLeader()
                .setBrokerId(n.id())
                .setHostName(n.host())
                .setPort(n.port())
            ).collect(Collectors.toList());

            LeaderAndIsrRequestData data = new LeaderAndIsrRequestData()
                .setControllerId(controllerId)
                .setControllerEpoch(controllerEpoch)
                .setBrokerEpoch(brokerEpoch)
                .setLiveLeaders(leaders);

            if (version >= 2) {
                Map<String, LeaderAndIsrTopicState> topicStatesMap = groupByTopic(partitionStates);
                data.setTopicStates(new ArrayList<>(topicStatesMap.values()));
            } else {
                data.setUngroupedPartitionStates(partitionStates);
            }

            return new LeaderAndIsrRequest(data, version);
        }

        private static Map<String, LeaderAndIsrTopicState> groupByTopic(List<LeaderAndIsrPartitionState> partitionStates) {
            Map<String, LeaderAndIsrTopicState> topicStates = new HashMap<>();
            // We don't null out the topic name in LeaderAndIsrRequestPartition since it's ignored by
            // the generated code if version >= 2
            for (LeaderAndIsrPartitionState partition : partitionStates) {
                LeaderAndIsrTopicState topicState = topicStates.computeIfAbsent(partition.topicName(),
                    t -> new LeaderAndIsrTopicState().setTopicName(partition.topicName()));
                topicState.partitionStates().add(partition);
            }
            return topicStates;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=LeaderAndIsRequest")
                .append(", controllerId=").append(controllerId)
                .append(", controllerEpoch=").append(controllerEpoch)
                .append(", brokerEpoch=").append(brokerEpoch)
                .append(", partitionStates=").append(partitionStates)
                .append(", liveLeaders=(").append(Utils.join(liveLeaders, ", ")).append(")")
                .append(")");
            return bld.toString();

        }
    }

    private final LeaderAndIsrRequestData data;

    LeaderAndIsrRequest(LeaderAndIsrRequestData data, short version) {
        super(ApiKeys.LEADER_AND_ISR, version);
        this.data = data;
        // Do this from the constructor to make it thread-safe (even though it's only needed when some methods are called)
        normalize();
    }

    private void normalize() {
        if (version() >= 2) {
            for (LeaderAndIsrTopicState topicState : data.topicStates()) {
                for (LeaderAndIsrPartitionState partitionState : topicState.partitionStates()) {
                    // Set the topic name so that we can always present the ungrouped view to callers
                    partitionState.setTopicName(topicState.topicName());
                }
            }
        }
    }

    public LeaderAndIsrRequest(Struct struct, short version) {
        this(new LeaderAndIsrRequestData(struct, version), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public LeaderAndIsrResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LeaderAndIsrResponseData responseData = new LeaderAndIsrResponseData();
        Errors error = Errors.forException(e);
        responseData.setErrorCode(error.code());

        List<LeaderAndIsrPartitionError> partitions = new ArrayList<>();
        for (LeaderAndIsrPartitionState partition : partitionStates()) {
            partitions.add(new LeaderAndIsrPartitionError()
                .setTopicName(partition.topicName())
                .setPartitionIndex(partition.partitionIndex())
                .setErrorCode(error.code()));
        }
        responseData.setPartitionErrors(partitions);
        return new LeaderAndIsrResponse(responseData);
    }

    @Override
    public int controllerId() {
        return data.controllerId();
    }

    @Override
    public int controllerEpoch() {
        return data.controllerEpoch();
    }

    @Override
    public long brokerEpoch() {
        return data.brokerEpoch();
    }

    public Iterable<LeaderAndIsrPartitionState> partitionStates() {
        if (version() >= 2)
            return () -> new FlattenedIterator<>(data.topicStates().iterator(),
                topicState -> topicState.partitionStates().iterator());
        return data.ungroupedPartitionStates();
    }

    public List<LeaderAndIsrLiveLeader> liveLeaders() {
        return Collections.unmodifiableList(data.liveLeaders());
    }

    // Visible for testing
    LeaderAndIsrRequestData data() {
        return data;
    }

    public static LeaderAndIsrRequest parse(ByteBuffer buffer, short version) {
        return new LeaderAndIsrRequest(ApiKeys.LEADER_AND_ISR.parseRequest(version, buffer), version);
    }
}
