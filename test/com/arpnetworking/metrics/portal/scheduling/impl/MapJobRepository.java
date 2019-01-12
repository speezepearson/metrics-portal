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

import com.arpnetworking.metrics.portal.scheduling.JobRepository;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import models.internal.Organization;
import models.internal.scheduling.Job;

import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * A simple in-memory {@link JobRepository}. Not in any way persistent, probably not good for production usage.
 *
 * @param <T> The type of the results computed by the repository's {@link Job}s.
 *
 * @author Spencer Pearson (spencerpearson at dropbox dot com)
 */
public final class MapJobRepository<T> implements JobRepository<T> {

    /**
     * Guice constructor.
     */
    @Inject
    public MapJobRepository() {}

    private final AtomicBoolean _open = new AtomicBoolean();
    private final Map<Organization, Map<UUID, Job<T>>> _jobs = Maps.newHashMap();
    private final Map<Organization, Map<UUID, Instant>> _lastRuns = Maps.newHashMap();

    @Override
    public void open() {
        assertIsClosed();
        LOGGER.debug().setMessage("opening JobRepository").log();
        _open.set(true);
    }

    @Override
    public void close() {
        assertIsOpen();
        LOGGER.debug().setMessage("closing JobRepository").log();
    }

    @Override
    public void addOrUpdateJob(final Job<T> job, final Organization organization) {
        assertIsOpen();
        _jobs.computeIfAbsent(organization, o -> Maps.newHashMap()).put(job.getId(), job);
    }

    @Override
    public Optional<Job<T>> getJob(final UUID id, final Organization organization) {
        assertIsOpen();
        return Optional.ofNullable(_jobs.getOrDefault(organization, Maps.newHashMap()).get(id));
    }

    @Override
    public Optional<Instant> getLastRun(final UUID id, final Organization organization) throws NoSuchElementException {
        assertIsOpen();
        return Optional.ofNullable(_lastRuns.getOrDefault(organization, Maps.newHashMap()).get(id));
    }

    @Override
    public Stream<Job<T>> getAllJobs() {
        return _jobs.values().stream().flatMap(m -> m.values().stream());
    }

    @Override
    public void jobStarted(final UUID id, final Organization organization, final Instant scheduled) {
        assertIsOpen();
        _lastRuns.computeIfAbsent(organization, o -> Maps.newHashMap())
                .compute(id, (id_, t1) -> (t1 == null) ? scheduled : t1.isAfter(scheduled) ? t1 : scheduled);
    }

    @Override
    public void jobSucceeded(final UUID id, final Organization organization, final Instant scheduled, final Object result) {
        assertIsOpen();
        _lastRuns.computeIfAbsent(organization, o -> Maps.newHashMap())
                .compute(id, (id_, t1) -> (t1 == null) ? scheduled : t1.isAfter(scheduled) ? t1 : scheduled);
    }

    @Override
    public void jobFailed(final UUID id, final Organization organization, final Instant scheduled, final Throwable error) {
        assertIsOpen();
        _lastRuns.computeIfAbsent(organization, o -> Maps.newHashMap())
                .compute(id, (id_, t1) -> (t1 == null) ? scheduled : t1.isAfter(scheduled) ? t1 : scheduled);
    }

    private void assertIsOpen() {
        assertIsOpen(true);
    }

    private void assertIsClosed() {
        assertIsOpen(false);
    }

    private void assertIsOpen(final boolean expectedState) {
        if (_open.get() != expectedState) {
            throw new IllegalStateException("MapJobRepository is not " + (expectedState ? "open" : "closed"));
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MapJobRepository.class);

}
