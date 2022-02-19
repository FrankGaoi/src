/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.Ignore;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Migration test from FlinkKafkaProducer011 operator. This test depends on the resource generated
 * by {@link FlinkKafkaProducer011MigrationTest#writeSnapshot()}.
 *
 * <p>Warning: We need to rename the generated resource based on the file naming pattern specified
 * by the {@link #getOperatorSnapshotPath(MigrationVersion)} method then copy the resource to the
 * path also specified by the {@link #getOperatorSnapshotPath(MigrationVersion)} method.
 */
public class FlinkKafkaProducerMigrationOperatorTest extends FlinkKafkaProducerMigrationTest {
    @Parameterized.Parameters(name = "Migration Savepoint: {0}")
    public static Collection<MigrationVersion> parameters() {
        return Arrays.asList(
                MigrationVersion.v1_8,
                MigrationVersion.v1_9,
                MigrationVersion.v1_10,
                MigrationVersion.v1_11);
    }

    public FlinkKafkaProducerMigrationOperatorTest(MigrationVersion testMigrateVersion) {
        super(testMigrateVersion);
    }

    @Override
    public String getOperatorSnapshotPath(MigrationVersion version) {
        return "src/test/resources/kafka-0.11-migration-kafka-producer-flink-"
                + version
                + "-snapshot";
    }

    @Ignore
    @Override
    public void writeSnapshot() throws Exception {
        throw new UnsupportedOperationException();
    }
}