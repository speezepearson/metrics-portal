package com.arpnetworking.rollups;

import com.arpnetworking.kairos.client.KairosDbClient;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

public class RollupEverythingDoer {
    private final KairosDbClient _kairosDbClient;
    private final RollupJobDatastore _jobDatastore;
    private final Semaphore _kairosDbRequestSemaphore;
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
        _jobDatastore.getLast(metric).thenAccept()
    }

    private CompletionStage<Void> rollupMetric(String metric) {
        return _jobDatastore.getFirstUndone(metric).thenCompose(job -> {
            if (!job.isPresent()) {
                return CompletableFuture.completedFuture(null);
            }
            return doRollupWithRetry(mkdef(job.get()))
                    .thenCompose(whatever ->
                        _jobDatastore.markDone(job.get()).thenCompose(whatever2 ->
                                rollupMetric(metric)
                        )
                    );
        });
    }

    private RollupDefinition mkdef(RollupJob rollupJob) {}
    private CompletionStage<Void> doRollupWithRetry(RollupDefinition defn) {

    }

    private static CompletableFuture<Void> allOf(Stream<CompletionStage<?>> futs) {
        return CompletableFuture.allOf(futs.map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new));
    }

    private interface RollupJobDatastore {
        CompletionStage<Optional<RollupJob>> getFirstUndone(String metric);
        CompletionStage<Void> markDone(RollupJob job);
        CompletionStage<Void> getLast(String metric);
    }
    private static class RollupJob {
        public final String sourceName;
        public final Instant time;
        public final RollupPeriod period;
        public final boolean done;
    }
}
