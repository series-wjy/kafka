# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from kafkatest.directory_layout.kafka_path import create_path_resolver, KafkaSystemTestPathResolver, \
    KAFKA_PATH_RESOLVER_KEY
<<<<<<< HEAD
from kafkatest.version import V_0_9_0_1, TRUNK, KafkaVersion
=======
from kafkatest.version import V_0_9_0_1, DEV_BRANCH, KafkaVersion
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b


class DummyContext(object):
    def __init__(self):
        self.globals = {}


class DummyPathResolver(object):
    """Dummy class to help check path resolver creation."""
    def __init__(self, context, project_name):
        pass

class DummyNode(object):
    """Fake node object"""
    pass

class CheckCreatePathResolver(object):
    def check_create_path_resolver_override(self):
        """Test override behavior when instantiating a path resolver using our factory function.

        If context.globals has an entry for a path resolver class, use that class instead of the default.
        """
        mock_context = DummyContext()
        mock_context.globals[KAFKA_PATH_RESOLVER_KEY] = \
            "unit.directory_layout.check_project_paths.DummyPathResolver"

        resolver = create_path_resolver(mock_context)
        assert type(resolver) == DummyPathResolver

    def check_create_path_resolver_default(self):
        """Test default behavior when instantiating a path resolver using our factory function.
        """
        resolver = create_path_resolver(DummyContext())
        assert type(resolver) == KafkaSystemTestPathResolver

    def check_paths(self):
        """Check expected path resolution without any version specified."""
        resolver = create_path_resolver(DummyContext())

<<<<<<< HEAD
        assert resolver.home() == "/opt/kafka-trunk"
        assert resolver.bin() == "/opt/kafka-trunk/bin"
        assert resolver.script("kafka-run-class.sh") == "/opt/kafka-trunk/bin/kafka-run-class.sh"
=======
        assert resolver.home() == "/opt/kafka-dev"
        assert resolver.bin() == "/opt/kafka-dev/bin"
        assert resolver.script("kafka-run-class.sh") == "/opt/kafka-dev/bin/kafka-run-class.sh"
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b

    def check_versioned_source_paths(self):
        """Check expected paths when using versions."""
        resolver = create_path_resolver(DummyContext())

        assert resolver.home(V_0_9_0_1) == "/opt/kafka-0.9.0.1"
        assert resolver.bin(V_0_9_0_1) == "/opt/kafka-0.9.0.1/bin"
        assert resolver.script("kafka-run-class.sh", V_0_9_0_1) == "/opt/kafka-0.9.0.1/bin/kafka-run-class.sh"

    def check_node_or_version_helper(self):
        """KafkaSystemTestPathResolver has a helper method which can take a node or version, and returns the version.
        Check expected behavior here.
        """
        resolver = create_path_resolver(DummyContext())

<<<<<<< HEAD
        # Node with no version attribute should resolve to TRUNK
        node = DummyNode()
        assert resolver._version(node) == TRUNK
=======
        # Node with no version attribute should resolve to DEV_BRANCH
        node = DummyNode()
        assert resolver._version(node) == DEV_BRANCH
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b

        # Node with version attribute should resolve to the version attribute
        node.version = V_0_9_0_1
        assert resolver._version(node) == V_0_9_0_1

        # A KafkaVersion object should resolve to itself
<<<<<<< HEAD
        assert resolver._version(TRUNK) == TRUNK
=======
        assert resolver._version(DEV_BRANCH) == DEV_BRANCH
>>>>>>> ce0b7f6373657d6bda208ff85a1c2c4fe8d05a7b
        version = KafkaVersion("999.999.999")
        assert resolver._version(version) == version


