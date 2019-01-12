/*
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
package com.arpnetworking.metrics.portal.scheduling.impl;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.InvalidActorNameException;
import akka.actor.Props;
import com.arpnetworking.metrics.portal.scheduling.JobRef;
import com.arpnetworking.metrics.portal.scheduling.JobRepository;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import models.internal.Organization;
import scala.Option;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

/**
 * todo.
 *
 * @param <T> todo.
 *
 * @author Spencer Pearson (spencerpearson at dropbox dot com)
 */
public final class JobSchedulerCoordinator<T> extends AbstractActorWithTimers {
    private final JobRepository<T> _repository;
    private final Organization _organization;

    /**
     * Props factory.
     *
     * @param <T> The type of result produced by the {@link JobRepository}'s jobs.
     * @param repository The job to intermittently execute.
     * @param organization The {@link Organization} whose jobs to coordinate.
     * @return A new props to create this actor.
     */
    public static <T> Props props(final JobRepository<T> repository, final Organization organization) {
        return Props.create(JobSchedulerCoordinator.class, () -> new JobSchedulerCoordinator<>(repository, organization));
    }

    /**
     * todo.
     * @param repository todo.
     * @param organization todo.
     */
    public JobSchedulerCoordinator(final JobRepository<T> repository, final Organization organization) {
        _repository = repository;
        _organization = organization;
        timers().startPeriodicTimer("TICK", Tick.INSTANCE, TICK_INTERVAL);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Tick.class, message -> {
                    _repository.getAllJobs(_organization).forEach(job -> {
                        final String actorId = "JobScheduler:" + job.getId();
                        final Option<ActorRef> ch = getContext().child(actorId);
                        if (!ch.isDefined()) {
                            final JobRef<T> ref = new JobRef.Builder<T>()
                                    .setRepository(_repository)
                                    .setOrganization(_organization)
                                    .setId(job.getId())
                                    .build();
                            LOGGER.info()
                                    .setMessage("no scheduler exists for job")
                                    .addData("ref", ref)
                                    .log();
                            getContext().actorOf(JobScheduler.props(ref));
                        }
                    });
                })
                .build();
    }


    private static final FiniteDuration TICK_INTERVAL = Duration.apply(1, TimeUnit.HOURS);
    private static final Logger LOGGER = LoggerFactory.getLogger(JobSchedulerCoordinator.class);

    /**
     * Internal message, telling the scheduler to run any necessary jobs.
     */
    protected static final class Tick {
        public static final Tick INSTANCE = new Tick();
    }

}
