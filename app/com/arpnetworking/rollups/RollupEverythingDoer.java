package com.arpnetworking.rollups;

import com.arpnetworking.kairos.client.KairosDbClient;
import com.arpnetworking.kairos.client.models.MetricTags;
import com.arpnetworking.kairos.client.models.TagsQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

public class RollupEverythingDoer {
    private final KairosDbClient _kairosDbClient;
    private final RollupJobDatastore _jobDatastore;
    private final Semaphore _kairosDbRequestSemaphore;

    public RollupEverythingDoer(final KairosDbClient _kairosDbClient, final RollupJobDatastore _jobDatastore, final Semaphore _kairosDbRequestSemaphore) {
        this._kairosDbClient = _kairosDbClient;
        this._jobDatastore = _jobDatastore;
        this._kairosDbRequestSemaphore = _kairosDbRequestSemaphore;
    }

    public void main() throws ExecutionException, InterruptedException {
        Instant nextRun = Instant.now();
        while (true) {
            final Instant now = Instant.now();
            while (nextRun.isBefore(now)) {
                nextRun = nextRun.plus(Duration.ofHours(30));
            }
            final List<String> metrics = _kairosDbClient.queryMetricNames().toCompletableFuture().get().getResults();
            allOf(metrics.stream().map(m -> populateTasks(m, now))).get();
            allOf(metrics.stream().map(this::rollupMetric)).get();
            Thread.sleep(ChronoUnit.MILLIS.between(Instant.now(), nextRun));
        }
    }

    private CompletionStage<Void> populateTasks(String metric, Instant now) {
        return _jobDatastore.getLast(metric).thenCompose(job -> {
            Set<Instant> times = Sets.newHashSet();
            Instant nextTime = job.map(RollupJob::getTime).orElse(Instant.now().minus(Duration.ofDays(7)).truncatedTo(ChronoUnit.HOURS));
            while (nextTime.isBefore(now)) {
                times.add(nextTime);
                nextTime = nextTime.plus(Duration.ofHours(1));
            }
            return allOf(times.stream().map(t -> _jobDatastore.add(new RollupJob(
                    metric,
                    t,
                    false
            ))));
        });
    }

    private CompletionStage<Void> rollupMetric(String metric) {
        return _jobDatastore.getFirstUndone(metric).thenCompose(job -> {
            if (!job.isPresent()) {
                return CompletableFuture.completedFuture(null);
            }
            return mkdef(job.get())
                    .thenCompose(this::doRollupWithRetry)
                    .thenCompose(whatever ->
                        _jobDatastore.markDone(job.get()).thenCompose(whatever2 ->
                                rollupMetric(metric)
                        )
                    );
        });
    }

    private CompletionStage<RollupDefinition> mkdef(RollupJob rollupJob) {
        return _kairosDbClient.queryMetricTags(new TagsQuery.Builder()
                .setMetrics(ImmutableList.of(new MetricTags.Builder().setName(rollupJob.sourceName).build()))
                .setStartTime(rollupJob.time)
                .setEndTime(rollupJob.time.plus(Duration.ofHours(1)).minusMillis(1))
                .build()
        ).thenApply(tags -> new RollupDefinition.Builder()
                .setSourceMetricName(rollupJob.sourceName)
                .setPeriod(RollupPeriod.HOURLY)
                .setStartTime(rollupJob.time)
                .setAllMetricTags(tags.getQueries().get(0).getResults().get(0).getTags())
                .build()
        );
    }
    private CompletionStage<Void> doRollupWithRetry(RollupDefinition defn) {

    }

    private static CompletableFuture<Void> allOf(Stream<CompletionStage<?>> futs) {
        return CompletableFuture.allOf(futs.map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new));
    }

    private interface RollupJobDatastore {
        CompletionStage<Optional<RollupJob>> getFirstUndone(String metric);
        CompletionStage<Void> markDone(RollupJob job);
        CompletionStage<Optional<RollupJob>> getLast(String metric);
        CompletionStage<Void> add(RollupJob job);
    }
    private static class RollupJob {
        public final String sourceName;
        public final Instant time;
        public final boolean done;

        public RollupJob(final String sourceName, final Instant time, final boolean done) {
            this.sourceName = sourceName;
            this.time = time;
            this.done = done;
        }

        public String getSourceName() {
            return sourceName;
        }

        public Instant getTime() {
            return time;
        }

        public boolean isDone() {
            return done;
        }
    }
}
