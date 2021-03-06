/*
 * Copyright 2020 Dropbox, Inc.
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

package com.arpnetworking.metrics.portal.alerts.impl;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Status;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.Patterns;
import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.portal.scheduling.Schedule;
import com.arpnetworking.metrics.portal.scheduling.impl.PeriodicSchedule;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.Sets;
import io.ebean.EbeanServer;
import io.ebean.SqlQuery;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.persistence.PersistenceException;

/**
 * An actor that will periodically create table partitions.
 *
 * @author Christian Briones (cbriones at dropbox dot com)
 */
public class DailyPartitionCreator extends AbstractActorWithTimers {
    /* package private */ static final Object TICK = "MSG_TICK";
    private static final Logger LOGGER = LoggerFactory.getLogger(DailyPartitionCreator.class);
    private static final Duration TICK_INTERVAL = Duration.ofMinutes(1);
    private static final String TICKER_NAME = "PERIODIC_TICK";

    private final EbeanServer _ebeanServer;
    private final PeriodicMetrics _periodicMetrics;
    private final Set<LocalDate> _partitionCache;

    private final int _lookaheadDays;
    private final String _schema;
    private final String _table;
    private final Clock _clock;
    private final Schedule _schedule;
    private Optional<Instant> _lastRun;

    private DailyPartitionCreator(
            final EbeanServer ebeanServer,
            final PeriodicMetrics periodicMetrics,
            final String schema,
            final String table,
            final Duration scheduleOffset,
            final int lookahead
    ) {
        this(ebeanServer, periodicMetrics, schema, table, scheduleOffset, lookahead, Clock.systemUTC());
    }

    /* package private */ DailyPartitionCreator(
            final EbeanServer ebeanServer,
            final PeriodicMetrics periodicMetrics,
            final String schema,
            final String table,
            final Duration scheduleOffset,
            final int lookaheadDays,
            final Clock clock
    ) {
        _ebeanServer = ebeanServer;
        _periodicMetrics = periodicMetrics;
        _lookaheadDays = lookaheadDays;
        _schedule = new PeriodicSchedule.Builder()
                .setOffset(scheduleOffset)
                .setPeriod(ChronoUnit.DAYS)
                .setRunAtAndAfter(Instant.EPOCH)
                .setZone(ZoneOffset.UTC)
                .build();
        _lastRun = Optional.empty();
        _schema = schema;
        _table = table;
        _clock = clock;
        _partitionCache = Sets.newHashSet();
    }

    /**
     * Create {@link Props} for this actor.
     *
     * @param ebeanServer the ebean server
     * @param periodicMetrics metrics instance to use
     * @param schema The database schema name
     * @param table The parent table name
     * @param scheduleOffset Execution offset from midnight
     * @param lookahead maximum number of partitions to create in advance
     * @return A new Props.
     */
    public static Props props(
            final EbeanServer ebeanServer,
            final PeriodicMetrics periodicMetrics,
            final String schema,
            final String table,
            final Duration scheduleOffset,
            final int lookahead
    ) {
        return Props.create(
                DailyPartitionCreator.class,
                () -> new DailyPartitionCreator(
                    ebeanServer,
                    periodicMetrics,
                    schema,
                    table,
                    scheduleOffset,
                    lookahead
                )
        );
    }

    /**
     * Ask the actor referenced by {@code ref} to create the partition(s) needed
     * for the given instant.
     *
     * @param ref an {@code DailyPartitionCreator}.
     * @param instant The instant being recorded
     * @param timeout timeout for the operation
     * @throws ExecutionException if an exception was thrown during execution.
     * @throws InterruptedException if the actor does not respond within the allotted timeout, or if the actor thread was
     * interrupted for other reasons.
     */
    public static void ensurePartitionExistsForInstant(
            final ActorRef ref,
            final Instant instant,
            final Duration timeout
    ) throws ExecutionException, InterruptedException {
        final LocalDate date = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).toLocalDate();
        final CreateForRange createPartitions = new CreateForRange.Builder()
                .setStart(date)
                .setEnd(date.plusDays(1))
                .build();
        Patterns.ask(
                ref,
                createPartitions,
                timeout
        )
        .toCompletableFuture()
        .get();
    }

    @Override
    public void preStart() {
        LOGGER.info().setMessage("Starting execution timer")
                .addData("schema", _schema)
                .addData("table", _table)
                .addData("lookahead", _lookaheadDays)
                .log();
        getSelf().tell(TICK, getSelf());
        getTimers().startPeriodicTimer(TICKER_NAME, TICK, TICK_INTERVAL);
    }


    @Override
    public void postStop() throws Exception {
        super.postStop();
        LOGGER.info().setMessage("Actor was stopped")
                .addData("schema", _schema)
                .addData("table", _table)
                .addData("lookahead", _lookaheadDays)
                .log();
    }

    @Override
    public Receive createReceive() {
        return new ReceiveBuilder()
                .matchEquals(TICK, msg -> tick())
                .match(CreateForRange.class, msg -> {
                    final Status.Status resp = execute(msg.getStart(), msg.getEnd());
                    getSender().tell(resp, getSelf());
                })
                .build();
    }

    private void recordCounter(final String metricName, final long value) {
        final String fullMetric = String.format("partition_creator/%s/%s", _table, metricName);
        _periodicMetrics.recordCounter(fullMetric, value);
    }

    // Message handlers

    private void tick() {
        recordCounter("tick", 1);

        final Instant now = _clock.instant();
        if (_schedule.nextRun(_lastRun).map(run -> run.isBefore(now)).orElse(true)) {
            final LocalDate startDate = ZonedDateTime.ofInstant(now, ZoneOffset.UTC).toLocalDate();
            final LocalDate endDate = startDate.plusDays(_lookaheadDays);

            final CreateForRange createPartitions = new CreateForRange.Builder()
                    .setStart(startDate)
                    .setEnd(endDate)
                    .build();
            getSelf().tell(createPartitions, ActorRef.noSender());
        }
    }

    private Status.Status execute(final LocalDate startDate, final LocalDate endDate) {

        // Much like other portions of the codebase dealing with time, the dates
        // used in this class are all fixed to UTC. So while the code in this
        // method uses a LocalDate, there's an implicit assumption that all
        // dates are UTC and these conversions happen at the interaction
        // boundary (tick, ensurePartitionExists).

        LocalDate d = startDate;
        boolean allPartitionsExist = true;
        while (!d.equals(endDate)) {
            if (!_partitionCache.contains(d)) {
                allPartitionsExist = false;
                break;
            }
            d = d.plusDays(1);
        }
        if (allPartitionsExist) {
            LOGGER.debug()
                    .setMessage("partitions already exist, ignoring execute request")
                    .addData("schema", _schema)
                    .addData("table", _table)
                    .addData("startDate", startDate)
                    .addData("endDate", endDate)
                    .log();
            return new Status.Success(null);
        }

        LOGGER.info()
                .setMessage("Creating daily partitions for table")
                .addData("schema", _schema)
                .addData("table", _table)
                .addData("startDate", startDate)
                .addData("endDate", endDate)
                .log();

        Status.Status status = new Status.Success(null);
        try {
            execute(_schema, _table, startDate, endDate);
            _lastRun = Optional.of(_clock.instant());
            updateCache(startDate, endDate);
        } catch (final PersistenceException e) {
            status = new Status.Failure(e);
            LOGGER.error()
                    .setMessage("Failed to create daily partitions for table")
                    .addData("schema", _schema)
                    .addData("table", _table)
                    .addData("startDate", startDate)
                    .addData("endDate", endDate)
                    .setThrowable(e)
                    .log();
        } finally {
            recordCounter("create", status instanceof Status.Success ? 0 : 1);
        }
        return status;
    }

    private void updateCache(final LocalDate start, final LocalDate end) {
        LocalDate date = start;
        while (!date.equals(end)) {
            _partitionCache.add(date);
            date = date.plusDays(1);
        }
    }

    /**
     * Create a series of daily partitions for the given parameters.
     *
     * @param schema the database schema
     * @param table the parent table
     * @param startDate the start date, inclusive.
     * @param endDate the end date, exclusive.
     */
    protected void execute(
            final String schema,
            final String table,
            final LocalDate startDate,
            final LocalDate endDate
    ) {
        // While this query does not return anything meaningful semantically,
        // it still returns a "non-empty" void result and so we can't use the
        // ordinarily more appropriate SqlUpdate type.
        final SqlQuery sql = _ebeanServer.createSqlQuery(
                "select * from create_daily_partition(?::text, ?::text, ?::date, ?::date)")
                .setParameter(1, schema)
                .setParameter(2, table)
                .setParameter(3, startDate)
                .setParameter(4, endDate);

        sql.findOneOrEmpty().orElseThrow(() -> new PersistenceException("Expected a single empty result."));
    }

    private static final class CreateForRange {
        private final LocalDate _start;
        private final LocalDate _end;

        private CreateForRange(final Builder builder) {
            _start = builder._start;
            _end = builder._end;
        }

        public LocalDate getStart() {
            return _start;
        }

        public LocalDate getEnd() {
            return _end;
        }

        static final class Builder extends OvalBuilder<CreateForRange> {
            private LocalDate _start;
            private LocalDate _end;

            Builder() {
                super(CreateForRange::new);
            }

            /**
             * Sets the start.
             *
             * @param start the start.
             * @return This instance of {@code Builder} for chaining.
             */
            public Builder setStart(final LocalDate start) {
                _start = start;
                return this;
            }

            /**
             * Sets the end.
             *
             * @param end the end.
             * @return This instance of {@code Builder} for chaining.
             */
            public Builder setEnd(final LocalDate end) {
                _end = end;
                return this;
            }
        }
    }
}
