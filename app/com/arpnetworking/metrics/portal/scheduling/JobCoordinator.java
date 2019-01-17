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
import com.google.inject.Key;
import com.google.inject.name.Names;
import models.internal.Organization;
import models.internal.scheduling.Job;

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
     * @param injector The Guice injector to load the {@link JobRepository} from.
     * @param repositoryType The type of the repository to load.
     * @param organization The {@link Organization} whose jobs to coordinate.
     * @return A new props to create this actor.
     */
    public static <T> Props props(final Injector injector,
                                  final Class<? extends JobRepository<T>> repositoryType,
                                  final Organization organization) {
        return props(injector, Clock.systemUTC(), repositoryType, organization);
    }

    /**
     * Props factory.
     *
     * @param <T> The type of result produced by the {@link JobRepository}'s jobs.
     * @param injector The Guice injector to load the {@link JobRepository} from.
     * @param clock The clock the actor will use to determine when the anti-entropy process should run.
     * @param repositoryType The type of the repository to load.
     * @param organization The {@link Organization} whose jobs to coordinate.
     * @return A new props to create this actor.
     */
    /* package-private */ static <T> Props props(final Injector injector,
                                                 final Clock clock,
                                                 final Class<? extends JobRepository<T>> repositoryType,
                                                 final Organization organization) {
        return Props.create(JobCoordinator.class, () -> new JobCoordinator<>(injector, clock, repositoryType, organization));
    }

    private JobCoordinator(final Injector injector,
                           final Clock clock,
                           final Class<? extends JobRepository<T>> repositoryType,
                           final Organization organization) {
        _injector = injector;
        _clock = clock;
        _repositoryType = repositoryType;
        _organization = organization;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        scheduleNextAntiEntropyTick(0);
    }

    private void scheduleNextAntiEntropyTick(final long nanosUntilTick) {
        timers().startSingleTimer("TICK", AntiEntropyTick.INSTANCE, scala.concurrent.duration.Duration.fromNanos(nanosUntilTick));
    }

    private void conditionallyReloadExecutor(final ActorRef executorRegion, final Job<T> job) {
        final JobRef<T> ref = new JobRef.Builder<T>()
                .setRepositoryType(_repositoryType)
                .setOrganization(_organization)
                .setId(job.getId())
                .build();
        executorRegion.tell(
                new JobExecutorActor.Envelope.Builder<T>()
                        .setJobRef(ref)
                        .setMessage(new JobExecutorActor.ConditionalReload.Builder().setETag(job.getETag()).build())
                        .build(),
                getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(AntiEntropyTick.class, message -> {
                    final Instant startTime = _clock.instant();

                    final JobRepository<T> repo = _injector.getInstance(_repositoryType);
                    final ActorRef executorRegion = _injector.getInstance(Key.get(
                            ActorRef.class,
                            Names.named("job-execution-shard-region")));
                    repo.getAllJobs(_organization).forEach(job -> this.conditionallyReloadExecutor(executorRegion, job));

                    // TODO(spencerpearson): also ensure that every executor corresponds to a job in the repo.

                    final Instant now = _clock.instant();
                    final Instant nextTickTime = startTime.plus(ANTI_ENTROPY_TICK_INTERVAL);
                    scheduleNextAntiEntropyTick(now.isBefore(nextTickTime) ? ChronoUnit.NANOS.between(now, nextTickTime) : 0);

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
