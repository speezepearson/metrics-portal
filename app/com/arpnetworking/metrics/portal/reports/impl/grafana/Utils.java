/*
 * Copyright 2019 Dropbox, Inc.
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

package com.arpnetworking.metrics.portal.reports.impl.grafana;

import com.arpnetworking.metrics.apachehttpsinkextra.shaded.org.apache.http.client.utils.URIBuilder;
import models.internal.impl.GrafanaReportPanelReportSource;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * TODO(spencerpearson).
 *
 * @author Spencer Pearson (spencerpearson at dropbox dot com)
 */
final class Utils {
    static URI getURI(final GrafanaReportPanelReportSource source, final Instant scheduled) {
        // TODO(spencerpearson): make this not always UTC -- maybe scheduled should be a ZonedDateTime?
        final ZonedDateTime alignedScheduled = ZonedDateTime.ofInstant(scheduled, ZoneOffset.UTC);
        final ZonedDateTime end = source.getTimeRangePeriod().addTo(alignedScheduled, -source.getTimeRangeEndPeriodsAgo());
        final ZonedDateTime start = source.getTimeRangePeriod().addTo(end, -source.getTimeRangeWidthPeriods());
        try {
            return new URIBuilder(source.getWebPageReportSource().getUri())
                    .addParameter("start", ((Long) (1000 * start.toEpochSecond())).toString())
                    .addParameter("end", ((Long) (1000 * end.toEpochSecond())).toString())
                    .build();
        } catch (final URISyntaxException e) {
            throw new RuntimeException("failed to build report URI", e);
        }
    }

    private Utils() {}
}
