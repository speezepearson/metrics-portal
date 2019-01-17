/**
 * Copyright 2018 Dropbox, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.metrics.portal.scheduling;

import akka.cluster.sharding.ShardRegion;

import javax.annotation.Nullable;

/**
 * Extracts data from messages to setup alert executors.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class JobMessageExtractor extends ShardRegion.HashCodeMessageExtractor {
    /**
     * Public constructor.
     */
    public JobMessageExtractor() {
        super(NUM_SHARDS);
    }

    @Override
    @Nullable
    public String entityId(final Object message) {
        if (message instanceof JobExecutorActor.Envelope) {
            return jobRefToUId(((JobExecutorActor.Envelope) message).getJobRef());
        }
        return null;
    }

    @Override
    @Nullable
    public Object entityMessage(final Object message) {
        if (message instanceof JobExecutorActor.Envelope) {
            return ((JobExecutorActor.Envelope) message).getMessage();
        }
        return null;
    }

    private static String jobRefToUId(final JobRef<?> ref) {
        return "repoType-" + ref.getRepositoryType()
                + "--orgId-" + ref.getOrganization().getId()
                + "--jobId-" + ref.getJobId();
    }

    private static final int NUM_SHARDS = 100;
}
