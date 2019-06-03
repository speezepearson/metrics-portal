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
package models.ebean;

import java.net.URI;
import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

/**
 * Data Model for SQL storage of a Grafana based report generation scheme.
 *
 * NOTE: This class is enhanced by Ebean to do things like lazy loading and
 * resolving relationships between beans. Therefore, including functionality
 * which serializes the state of the object can be dangerous (e.g. {@code toString},
 * {@code @Loggable}, etc.).
 *
 * @author Spencer Pearson (spencerpearson at dropbox dot com)
 */
// CHECKSTYLE.OFF: MemberNameCheck
@Entity
@DiscriminatorValue("GRAFANA")
public class GrafanaReportPanelReportSource extends ReportSource {
    @Column(name = "url")
    private URI uri;

    @Column(name = "title")
    private String title;

    @Column(name = "ignore_certificate_errors")
    private boolean ignoreCertificateErrors;

    @Column(name = "triggering_event_name")
    private String triggeringEventName;

    public URI getUri() {
        return uri;
    }

    public void setUri(final URI value) {
        uri = value;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(final String value) {
        title =  value;
    }

    public boolean isIgnoreCertificateErrors() {
        return ignoreCertificateErrors;
    }

    public void setIgnoreCertificateErrors(final boolean value) {
        ignoreCertificateErrors = value;
    }

    public String getTriggeringEventName() {
        return triggeringEventName;
    }

    public void setTriggeringEventName(final String value) {
        triggeringEventName = value;
    }

    @Override
    public models.internal.impl.GrafanaReportPanelReportSource toInternal() {
        return new models.internal.impl.GrafanaReportPanelReportSource.Builder().setWebPageReportSource(
                new models.internal.impl.WebPageReportSource.Builder()
                    .setId(getUuid())
                    .setUri(uri)
                    .setTitle(title)
                    .setIgnoreCertificateErrors(ignoreCertificateErrors)
                    .setTriggeringEventName(triggeringEventName)
                    .build()
        ).build();
    }
}
// CHECKSTYLE.ON: MemberNameCheck
