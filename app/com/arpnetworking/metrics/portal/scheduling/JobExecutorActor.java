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
import com.arpnetworking.metrics.Unit;
import com.arpnetworking.metrics.impl.BaseScale;
import com.arpnetworking.metrics.impl.BaseUnit;
import com.arpnetworking.metrics.impl.TsdUnit;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.incubator.impl.TsdPeriodicMetrics;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Injector;
import models.internal.Organization;
import models.internal.scheduling.Job;
import net.sf.oval.constraint.NotNull;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
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
    private final Clock _clock;
    private final PeriodicMetrics _periodicMetrics;

    @Nullable
    private JobRef<T> _jobRef = null;
    private Optional<Job<T>> _cachedJob = Optional.empty();

    /**
     * Props factory.
     *
     * @param <T> The type of result produced by the {@link JobRef}'s job.
     * @param injector The Guice injector to use to load the {@link JobRepository} referenced by the {@link JobRef}.
     * @param clock The clock the scheduler will use, when it ticks, to determine whether it's time to run the next job(s) yet.
     * @param periodicMetrics The {@link PeriodicMetrics} that this actor will use to log its metrics.
     * @return A new props to create this actor.
     */
    public static <T> Props props(final Injector injector, final Clock clock, final PeriodicMetrics periodicMetrics) {
        return Props.create(JobExecutorActor.class, () -> new JobExecutorActor<>(injector, clock, periodicMetrics));
    }

    private JobExecutorActor(final Injector injector, final Clock clock, final PeriodicMetrics periodicMetrics) {
        _injector = injector;
        _clock = clock;
        _periodicMetrics = periodicMetrics;
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
        final FiniteDuration timeRemaining = timeUntilNextTick(wakeUpBy);
        _periodicMetrics.recordTimer(
                "job_executor_actor_time_remaining",
                timeRemaining.toNanos(),
                Optional.of(NANOS));
        timers().startSingleTimer("TICK", Tick.INSTANCE, timeRemaining);
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

    private void reloadCachedJobIfOutdated(final String upToDateETag) {
        if (_cachedJob.isPresent() && _cachedJob.get().getETag().equals(upToDateETag)) {
            return;
        }
        LOGGER.debug()
                .setMessage("job is stale; reloading")
                .addData("jobRef", _jobRef)
                .addData("oldETag", _cachedJob.map(Job::getETag).orElse("<none>"))
                .addData("newETag", upToDateETag)
                .log();
        reloadCachedJob();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Tick.class, message -> {
                    _periodicMetrics.recordCounter("job-executor-actor-ticks", 1);

                    if (_jobRef == null) {
                        LOGGER.error()
                                .setMessage("uninitialized JobExecutorActor is trying to tick")
                                .log();
                    }

                    if (!_cachedJob.isPresent()) {
                        reloadCachedJob();
                    }
                    final Optional<Job<T>> job = _cachedJob; // local to ensure it doesn't change mid-message-handling
                    if (!job.isPresent()) {
                        LOGGER.warn()
                                .setMessage("no such job")
                                .addData("jobRef", _jobRef)
                                .log();
                        getSelf().tell(PoisonPill.getInstance(), getSelf());
                        return;
                    }

                    final JobRepository<T> repo = _jobRef.getRepository(_injector);

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
                .match(Initialize.class, message -> {
                    if (_jobRef != null) {
                        LOGGER.error()
                                .setMessage("trying to initialize an already-initialized JobExecutorActor")
                                .addData("currentRef", _jobRef)
                                .addData("newRef", message.getJobRef())
                                .log();
                        return;
                    }

                    LOGGER.info()
                            .setMessage("initializing JobExecutorActor")
                            .addData("jobRef", message.getJobRef())
                            .log();

                    // THIS MAKES ME SO SAD. But there's simply no way to plumb the type information through Akka.
                    @SuppressWarnings("unchecked")
                    final JobRef<T> ref = message.getJobRef();

                    _jobRef = ref;
                    getSelf().tell(UnconditionalReload.getInstance(), getSelf());
                    getSelf().tell(Tick.INSTANCE, getSelf());
                })
                .match(UnconditionalReload.class, message -> this.reloadCachedJob())
                .match(ConditionalReload.class, message -> reloadCachedJobIfOutdated(message.getETag()))
                .build();
    }

    @Override
    public Receive createReceiveRecover() {
        return receiveBuilder().build();
    }

    @Override
    public String persistenceId() {
        final String id = (_jobRef == null) ? "" : _jobRef.getJobId().toString();
        return "com.arpnetworking.metrics.portal.scheduling.JobExecutorActor:" + id;
    }

    /* package private */ static final FiniteDuration TICK_INTERVAL = Duration.apply(1, TimeUnit.MINUTES);
    private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutorActor.class);
    private static final Unit NANOS = new TsdUnit.Builder()
            .setScale(BaseScale.NANO)
            .setBaseUnit(BaseUnit.SECOND)
            .build();

    /**
     * Internal message, telling the scheduler to run any necessary jobs.
     * Intended only for internal use and testing.
     */
    /* package private */ static final class Tick implements Serializable {
        /* package private */ static final Tick INSTANCE = new Tick();
        private static final long serialVersionUID = 1L;

    }

    /**
     * Wraps a message in routing information so that Akka's cluster-sharding can route it to the right {@link JobExecutorActor}.
     *
     * @param <T> The type of result of the job run by the recipient actor.
     */
    public static final class Envelope<T> implements Serializable {
        private final JobRef<T> _jobRef;
        private final Object _message;

        private Envelope(final Builder<T> builder) {
            _jobRef = builder._jobRef;
            _message = builder._message;
        }

        public JobRef<T> getJobRef() {
            return _jobRef;
        }

        public Object getMessage() {
            return _message;
        }

        private static final long serialVersionUID = 1L;

        /**
         * Implementation of builder pattern for {@link Envelope}.
         *
         * @param <T> The type of result of the job run by the recipient actor.
         *
         * @author Spencer Pearson (spencerpearson at dropbox dot com)
         */
        public static final class Builder<T> extends OvalBuilder<Envelope<T>> {
            @NotNull
            private JobRef<T> _jobRef;
            @NotNull
            private Object _message;

            /**
             * Public constructor.
             */
            Builder() {
                super(Envelope<T>::new);
            }

            /**
             * The {@link JobRef} of the actor that should receive the message. Required. Must not be null.
             *
             * @param jobRef The reference to the job whose actor to contact.
             * @return This instance of Builder.
             */
            public Builder<T> setJobRef(final JobRef<T> jobRef) {
                _jobRef = jobRef;
                return this;
            }

            /**
             * The Akka message to be sent to the {@link JobExecutorActor}. Required. Must not be null.
             *
             * @param message The message.
             * @return This instance of Builder.
             */
            public Builder<T> setMessage(final Object message) {
                _message = message;
                return this;
            }
        }
    }

    /**
     * Initializes the actor with a {@link JobRef} identifying its job.
     *
     * Must be the first message received. Must not be received more than once.
     *
     * @param <T> The type of the result of the job that the recipient should run.
     */
    public static final class Initialize<T> implements Serializable {

        private final JobRef<T> _jobRef;

        private static final long serialVersionUID = 1L;

        private Initialize(final Builder<T> builder) {
            _jobRef = builder._jobRef;
        }

        public JobRef<T> getJobRef() {
            return _jobRef;
        }

        /**
         * Implementation of builder pattern for {@link Initialize}.
         *
         * @param <T> The type of the result of the job that the recipient should run.
         *
         * @author Spencer Pearson (spencerpearson at dropbox dot com)
         */
        public static final class Builder<T> extends OvalBuilder<Initialize<T>> {
            @NotNull
            private JobRef<T> _jobRef;

            /**
             * Public constructor.
             */
            Builder() {
                super(Initialize<T>::new);
            }

            /**
             * The {@link JobRef} to initialize this actor with. Required. Must not be null.
             *
             * @param jobRef The reference to the job whose actor to contact.
             * @return This instance of Builder.
             */
            public Builder<T> setJobRef(final JobRef<T> jobRef) {
                _jobRef = jobRef;
                return this;
            }
        }
    }

    /**
     * Commands the actor to reload its job from its repository.
     */
    public static final class UnconditionalReload implements Serializable {
        private static final UnconditionalReload INSTANCE = new UnconditionalReload();

        public static UnconditionalReload getInstance() {
            return INSTANCE;
        }

        private static final long serialVersionUID = 1L;
    }

    /**
     * Commands the actor to reload its job from its repository, if the cached ETag is out of date.
     */
    public static final class ConditionalReload implements Serializable {
        private final String _eTag;

        private ConditionalReload(final Builder builder) {
            _eTag = builder._eTag;
        }

        public String getETag() {
            return _eTag;
        }

        private static final long serialVersionUID = 1L;

        /**
         * Implementation of builder pattern for {@link ConditionalReload}.
         *
         * @author Spencer Pearson (spencerpearson at dropbox dot com)
         */
        public static final class Builder extends OvalBuilder<ConditionalReload> {
            @NotNull
            private String _eTag;

            /**
             * Public constructor.
             */
            Builder() {
                super(ConditionalReload::new);
            }

            /**
             * Causes the actor not to reload if it equals the actor's current ETag. Required. Cannot be null.
             *
             * @param eTag The ETag.
             * @return This instance of Builder.
             */
            public Builder setETag(@Nullable final String eTag) {
                _eTag = eTag;
                return this;
            }
        }
    }
}
