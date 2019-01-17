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

import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.persistence.AbstractPersistentActorWithTimers;
import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.incubator.impl.TsdPeriodicMetrics;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.inject.Injector;
import models.internal.Organization;
import models.internal.scheduling.Job;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * An actor that executes {@link Job}s.
 *
 * @param <T> The type of result produced by the {@link Job}s.
 *
 * @author Spencer Pearson (spencerpearson at dropbox dot com)
 */
public final class JobExecutorActor<T> extends AbstractPersistentActorWithTimers {

    private final Injector _injector;
    private final JobRef<T> _jobRef;
    private final Clock _clock;
    private final PeriodicMetrics _periodicMetrics;

    private Optional<Job<T>> _cachedJob = Optional.empty();

    /**
     * Props factory.
     *
     * @param <T> The type of result produced by the {@link JobRef}'s job.
     * @param injector The Guice injector to use to load the {@link JobRepository} referenced by the {@link JobRef}.
     * @param jobRef The job to intermittently execute.
     * @return A new props to create this actor.
     */
    public static <T> Props props(final Injector injector, final JobRef<T> jobRef) {
        return props(injector, jobRef, Clock.systemUTC());
    }

    /**
     * Props factory.
     *
     * @param <T> The type of result produced by the {@link JobRef}'s job.
     * @param injector The Guice injector to use to load the {@link JobRepository} referenced by the {@link JobRef}.
     * @param jobRef The job to intermittently execute.
     * @param clock The clock the scheduler will use, when it ticks, to determine whether it's time to run the next job(s) yet.
     * @return A new props to create this actor.
     */
    protected static <T> Props props(final Injector injector, final JobRef<T> jobRef, final Clock clock) {
        return Props.create(JobExecutorActor.class, () -> new JobExecutorActor<>(injector, jobRef, clock));
    }

    private JobExecutorActor(final Injector injector, final JobRef<T> jobRef, final Clock clock) {
        _injector = injector;
        _jobRef = jobRef;
        _clock = clock;
        _periodicMetrics = new TsdPeriodicMetrics.Builder()
                .setMetricsFactory(injector.getInstance(MetricsFactory.class))
                .build();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        getSelf().tell(Reload.FORCE, getSelf());
        getSelf().tell(Tick.INSTANCE, getSelf());
    }

    /**
     * Returns the time until the actor should next wake up.
     *
     * @param wakeUpBy The time we want to be sure we wake up by.
     * @return The time until we should wake up. Always between 0 and (approximately) {@code wakeUpBy}.
     */
    /* package private */ FiniteDuration timeUntilNextTick(final Instant wakeUpBy) {
        final FiniteDuration delta = Duration.fromNanos(ChronoUnit.NANOS.between(_clock.instant(), wakeUpBy));
        if (delta.lt(Duration.Zero())) {
            return Duration.Zero();
        } else if (delta.lt(TICK_INTERVAL)) {
            return delta;
        } else {
            return TICK_INTERVAL;
        }
    }

    private void scheduleNextTick(final Instant wakeUpBy) {
        timers().startSingleTimer("TICK", Tick.INSTANCE, timeUntilNextTick(wakeUpBy));
    }

    private void executeAndScheduleNextTick(
            final JobRepository<T> repo,
            final Organization org,
            final Job<T> job,
            final Instant scheduled
    ) {
        final Optional<Instant> nextScheduled = job.getSchedule().nextRun(Optional.of(scheduled));
        repo.jobStarted(job.getId(), org, scheduled);
        job.execute(getSelf(), scheduled)
                .handle((result, error) -> {
                    if (error == null) {
                        repo.jobSucceeded(job.getId(), org, scheduled, result);
                    } else {
                        repo.jobFailed(job.getId(), org, scheduled, error);
                    }
                    return null;
                })
                .thenApply(whatever -> {
                    nextScheduled.ifPresent(this::scheduleNextTick);
                    return null;
                });
    }

    private void reloadCachedJob() {
        _cachedJob = _jobRef.get(_injector);
    }

    private void reloadCachedJobIfOutdated(final Optional<String> currentETag) {
        if (!currentETag.isPresent()
            || !_cachedJob.isPresent()
            || !_cachedJob.get().getETag().equals(currentETag.get())) {
            reloadCachedJob();
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Tick.class, message -> {
                    _periodicMetrics.recordCounter("job-executor-actor-ticks", 1);

                    final Optional<Job<T>> job = _cachedJob; // local to ensure it doesn't change mid-message-handling

                    final JobRepository<T> repo = _jobRef.getRepository(_injector);
                    if (!job.isPresent()) {
                        LOGGER.warn()
                                .setMessage("no such job")
                                .addData("jobRef", _jobRef)
                                .log();
                        getSelf().tell(PoisonPill.getInstance(), getSelf());
                        return;
                    }

                    final UUID id = _jobRef.getJobId();
                    final Organization org = _jobRef.getOrganization();
                    final Optional<Instant> lastRun = repo.getLastRun(id, org);
                    final Optional<Instant> nextRun = job.get().getSchedule().nextRun(lastRun);
                    if (!nextRun.isPresent()) {
                        LOGGER.info()
                                .setMessage("job has no more scheduled runs")
                                .addData("jobRef", _jobRef)
                                .addData("schedule", job.get().getSchedule())
                                .addData("lastRun", lastRun)
                                .log();
                        getSelf().tell(PoisonPill.getInstance(), getSelf());
                        return;
                    }

                    if (!_clock.instant().isBefore(nextRun.get())) {
                        executeAndScheduleNextTick(repo, org, job.get(), nextRun.get());
                    } else {
                        scheduleNextTick(nextRun.get());
                    }
                })
                .match(Reload.class, message -> {
                    reloadCachedJobIfOutdated(message.getETag());
                })
                .build();
    }

    @Override
    public Receive createReceiveRecover() {
        return receiveBuilder().build();
    }

    @Override
    public String persistenceId() {
        return "com.arpnetworking.metrics.portal.scheduling.JobExecutorActor:" + _jobRef.getJobId();
    }

    /* package private */ static final FiniteDuration TICK_INTERVAL = Duration.apply(1, TimeUnit.MINUTES);
    private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutorActor.class);

    /**
     * Internal message, telling the scheduler to run any necessary jobs.
     * Intended only for internal use and testing.
     */
    /* package private */ static final class Tick {
        public static final Tick INSTANCE = new Tick();
    }

    /**
     * Commands the actor to reload its job from its repository, if the cached ETag is out of date.
     */
    /* package private */ static final class Reload {
        private final Optional<String> _eTag;
        public static final Reload FORCE = new Builder().build();

        private Reload(final Builder builder) {
            _eTag = builder._eTag;
        }

        public Optional<String> getETag() {
            return _eTag;
        }

        /**
         * Implementation of builder pattern for {@link Reload}.
         *
         * @author Spencer Pearson (spencerpearson at dropbox dot com)
         */
        public static final class Builder extends OvalBuilder<Reload> {
            private Optional<String> _eTag = null;

            /**
             * Public constructor.
             */
            Builder() {
                super(Reload::new);
            }

            /**
             * Causes the actor not to reload if it equals the actor's current ETag. Optional. Defaults to null.
             *
             * @param eTag The ETag.
             * @return This instance of Builder.
             */
            public Builder setETag(@Nullable final String eTag) {
                _eTag = Optional.ofNullable(eTag);
                return this;
            }
        }
    }
}
