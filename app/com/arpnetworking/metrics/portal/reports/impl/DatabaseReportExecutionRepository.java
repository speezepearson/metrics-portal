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
package com.arpnetworking.metrics.portal.reports.impl;

import com.arpnetworking.metrics.portal.scheduling.JobExecutionRepository;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.base.Throwables;
import io.ebean.EbeanServer;
import io.ebean.Transaction;
import models.ebean.ReportExecution;
import models.internal.Organization;
import models.internal.reports.Report;
import models.internal.scheduling.JobExecution;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.EntityNotFoundException;
import javax.persistence.PersistenceException;

/**
 * Implementation of {@link JobExecutionRepository} for {@link Report} jobs using a SQL database.
 *
 * @author Christian Briones (cbriones at dropbox dot com)
 */
public final class DatabaseReportExecutionRepository implements JobExecutionRepository<Report.Result> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseReportExecutionRepository.class);

    private final AtomicBoolean _isOpen = new AtomicBoolean(false);
    private final EbeanServer _ebeanServer;

    /**
     * Public constructor.
     *
     * @param ebeanServer Play's {@code EbeanServer} for this repository.
     */
    @Inject
    public DatabaseReportExecutionRepository(@Named("metrics_portal") final EbeanServer ebeanServer) {
        _ebeanServer = ebeanServer;
    }

    @Override
    public void open() {
        assertIsOpen(false);
        LOGGER.debug().setMessage("Opening DatabaseReportExecutionRepository").log();
        _isOpen.set(true);
    }

    @Override
    public void close() {
        assertIsOpen();
        LOGGER.debug().setMessage("Closing DatabaseReportExecutionRepository").log();
        _isOpen.set(false);
    }


    /**
     * Get the most recently scheduled execution, if any.
     * <p>
     * This could possibly return an execution that's pending completion.
     *
     * @param jobId The UUID of the job that completed.
     * @param organization The organization owning the job.
     * @return The last successful execution.
     * @throws NoSuchElementException if no job has the given UUID.
     */
    public Optional<JobExecution<Report.Result>> getLastScheduled(final UUID jobId, final Organization organization)
            throws NoSuchElementException {
        assertIsOpen();
        return _ebeanServer.find(ReportExecution.class)
                .orderBy()
                .desc("scheduled")
                .where()
                .eq("report.uuid", jobId)
                .eq("report.organization.uuid", organization.getId())
                .setMaxRows(1)
                .findOneOrEmpty()
                .map(this::toInternalModel);
    }

    @Override
    public Optional<JobExecution.Success<Report.Result>> getLastSuccess(final UUID jobId, final Organization organization)
            throws NoSuchElementException {
        assertIsOpen();
        final Optional<ReportExecution> row = _ebeanServer.find(ReportExecution.class)
                .orderBy()
                .desc("completed_at")
                .where()
                .eq("report.uuid", jobId)
                .eq("report.organization.uuid", organization.getId())
                .eq("state", ReportExecution.State.SUCCESS)
                .setMaxRows(1)
                .findOneOrEmpty();
        if (row.isPresent()) {
            final JobExecution<Report.Result> execution = toInternalModel(row.get());
            if (execution instanceof JobExecution.Success) {
                return Optional.of((JobExecution.Success<Report.Result>) execution);
            }
            throw new IllegalStateException(
                    String.format("execution returned was not a success when specified by the query: %s", row.get())
            );
        }
        return Optional.empty();
    }

    @Override
    public Optional<JobExecution<Report.Result>> getLastCompleted(final UUID jobId, final Organization organization)
            throws NoSuchElementException {
        assertIsOpen();
        return _ebeanServer.find(ReportExecution.class)
                .orderBy()
                .desc("completed_at")
                .where()
                .eq("report.uuid", jobId)
                .eq("report.organization.uuid", organization.getId())
                .in("state", ReportExecution.State.SUCCESS, ReportExecution.State.FAILURE)
                .setMaxRows(1)
                .findOneOrEmpty()
                .map(this::toInternalModel);
    }

    @Override
    public void jobStarted(final UUID reportId, final Organization organization, final Instant scheduled) {
        assertIsOpen();
        updateExecutionState(
                reportId,
                organization,
                scheduled,
                ReportExecution.State.STARTED,
                execution -> {
                    execution.setStartedAt(Instant.now());
                }
        );
    }

    @Override
    public void jobSucceeded(final UUID reportId, final Organization organization, final Instant scheduled, final Report.Result result) {
        assertIsOpen();
        updateExecutionState(
                reportId,
                organization,
                scheduled,
                ReportExecution.State.SUCCESS,
                execution -> {
                    execution.setResult(result);
                    execution.setCompletedAt(Instant.now());
                }
        );
    }

    @Override
    public void jobFailed(final UUID reportId, final Organization organization, final Instant scheduled, final Throwable error) {
        assertIsOpen();
        updateExecutionState(
                reportId,
                organization,
                scheduled,
                ReportExecution.State.FAILURE,
                execution -> {
                    execution.setError(Throwables.getStackTraceAsString(error));
                    execution.setCompletedAt(Instant.now());
                }
        );
    }

    private void updateExecutionState(
            final UUID reportId,
            final Organization organization,
            final Instant scheduled,
            final ReportExecution.State state,
            final Consumer<ReportExecution> update
    ) {
        LOGGER.debug()
                .setMessage("Upserting report execution")
                .addData("report.uuid", reportId)
                .addData("scheduled", scheduled)
                .addData("state", state)
                .log();
        try (Transaction transaction = _ebeanServer.beginTransaction()) {
            final Optional<models.ebean.Report> report = models.ebean.Organization.findByOrganization(_ebeanServer, organization)
                    .flatMap(beanOrg -> models.ebean.Report.findByUUID(
                            _ebeanServer,
                            beanOrg,
                            reportId
                    ));
            if (!report.isPresent()) {
                final String message = String.format(
                        "Could not find report with uuid=%s, organization.uuid=%s",
                        reportId,
                        organization.getId()
                );
                throw new EntityNotFoundException(message);
            }

            final Optional<ReportExecution> existingExecution = report.flatMap(r ->
                    _ebeanServer.createQuery(ReportExecution.class)
                            .where()
                            .eq("report", r)
                            .eq("scheduled", scheduled)
                            .findOneOrEmpty()
            );
            final ReportExecution newOrUpdatedExecution = existingExecution.orElse(new ReportExecution());
            newOrUpdatedExecution.setReport(report.get());
            newOrUpdatedExecution.setScheduled(scheduled);

            update.accept(newOrUpdatedExecution);
            newOrUpdatedExecution.setState(state);

            if (existingExecution.isPresent()) {
                _ebeanServer.update(newOrUpdatedExecution);
            } else {
                _ebeanServer.save(newOrUpdatedExecution);
            }

            LOGGER.debug()
                    .setMessage("Upserted report execution")
                    .addData("report.uuid", reportId)
                    .addData("scheduled", scheduled)
                    .addData("state", state)
                    .log();
            transaction.commit();
            // CHECKSTYLE.OFF: IllegalCatchCheck
        } catch (final RuntimeException e) {
            // CHECKSTYLE.ON: IllegalCatchCheck
            LOGGER.error()
                    .setMessage("Failed to upsert report executions")
                    .addData("report.uuid", reportId)
                    .addData("scheduled", scheduled)
                    .addData("state", state)
                    .setThrowable(e)
                    .log();
            throw new PersistenceException("Failed to upsert report executions", e);
        }
    }

    private JobExecution<Report.Result> toInternalModel(final ReportExecution beanModel) {
        final ReportExecution.State state = beanModel.getState();
        switch (state) {
            case STARTED:
                return new JobExecution.Started.Builder<Report.Result>()
                        .setJobId(beanModel.getReport().getUuid())
                        .setScheduled(beanModel.getScheduled())
                        .setStartedAt(beanModel.getStartedAt())
                        .build();
            case FAILURE:
                @Nullable final Throwable throwable = beanModel.getError() == null ? null : new Throwable(beanModel.getError());
                return new JobExecution.Failure.Builder<Report.Result>()
                        .setJobId(beanModel.getReport().getUuid())
                        .setScheduled(beanModel.getScheduled())
                        .setStartedAt(beanModel.getStartedAt())
                        .setCompletedAt(beanModel.getCompletedAt())
                        .setError(throwable)
                        .build();
            case SUCCESS:
                return new JobExecution.Success.Builder<Report.Result>()
                        .setJobId(beanModel.getReport().getUuid())
                        .setScheduled(beanModel.getScheduled())
                        .setCompletedAt(beanModel.getCompletedAt())
                        .setStartedAt(beanModel.getStartedAt())
                        .setResult(beanModel.getResult())
                        .build();
            default:
                throw new AssertionError("unexpected state: " + state);
        }
    }

    private void assertIsOpen() {
        assertIsOpen(true);
    }

    private void assertIsOpen(final boolean expectedState) {
        if (_isOpen.get() != expectedState) {
            throw new IllegalStateException(String.format("DatabaseReportExecutionRepository is not %s",
                    expectedState ? "open" : "closed"));
        }
    }
}
