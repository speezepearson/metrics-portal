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
package com.arpnetworking.metrics.portal.scheduling;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.inject.Injector;
import models.internal.Organization;
import scala.Option;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * todo.
 *
 * @param <T> todo.
 *
 * @author Spencer Pearson (spencerpearson at dropbox dot com)
 */
public final class JobCoordinator<T> extends AbstractActorWithTimers {
    private final Injector _injector;
    private final Clock _clock;
    private final Class<? extends JobRepository<T>> _repositoryType;
    private final Organization _organization;

    /**
     * Props factory.
     *
     * @param <T> The type of result produced by the {@link JobRepository}'s jobs.
     * @param repositoryType The job to intermittently execute.
     * @param organization The {@link Organization} whose jobs to coordinate.
     * @return A new props to create this actor.
     */
    public static <T> Props props(final Injector injector, final Clock clock, final Class<? extends JobRepository<T>> repositoryType, final Organization organization) {
        return Props.create(JobCoordinator.class, () -> new JobCoordinator<>(injector, clock, repositoryType, organization));
    }

    /**
     * todo.
     * @param injector todo.
     * @param clock todo.
     * @param repositoryType todo.
     * @param organization todo.
     */
    private JobCoordinator(final Injector injector, final Clock clock, final Class<? extends JobRepository<T>> repositoryType, final Organization organization) {
        _injector = injector;
        _clock = clock;
        _repositoryType = repositoryType;
        _organization = organization;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        scheduleNextTick(0);
    }

    private void scheduleNextTick(long nanosUntilTick) {
        timers().startSingleTimer("TICK", AntiEntropyTick.INSTANCE, scala.concurrent.duration.Duration.fromNanos(nanosUntilTick));
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(AntiEntropyTick.class, message -> {
                    final Instant startTime = _clock.instant();
                    final JobRepository<T> repo = _injector.getInstance(_repositoryType);
                    repo.getAllJobs(_organization).forEach(job -> {
                        final String actorId = "JobExecutorActor:" + job.getId();
                        final Option<ActorRef> ch = getContext().child(actorId);
                        if (ch.isDefined()) {
                            ch.get().tell(
                                    new JobExecutorActor.Reload.Builder()
                                            .setETag(job.getETag())
                                            .build(),
                                    getSelf());
                        } else {
                            final JobRef<T> ref = new JobRef.Builder<T>()
                                    .setRepositoryType(_repositoryType)
                                    .setOrganization(_organization)
                                    .setId(job.getId())
                                    .build();
                            LOGGER.info()
                                    .setMessage("no scheduler exists for job")
                                    .addData("ref", ref)
                                    .log();
                            getContext().actorOf(JobExecutorActor.props(_injector, ref));
                        }
                    });
                    final Instant now = _clock.instant();
                    final Instant nextTickTime = startTime.plus(ANTI_ENTROPY_TICK_INTERVAL);
                    scheduleNextTick(now.isBefore(nextTickTime) ? ChronoUnit.NANOS.between(now, nextTickTime) : 0);
                })
                .build();
    }

    private static final Duration ANTI_ENTROPY_TICK_INTERVAL = Duration.ofHours(1);
    private static final Logger LOGGER = LoggerFactory.getLogger(JobCoordinator.class);

    /**
     * Internal message, telling the scheduler to run any necessary jobs.
     */
    protected static final class AntiEntropyTick {
        /* package private */ static final AntiEntropyTick INSTANCE = new AntiEntropyTick();
    }

}
