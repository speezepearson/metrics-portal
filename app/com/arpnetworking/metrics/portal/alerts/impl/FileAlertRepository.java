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

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.metrics.portal.alerts.AlertRepository;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.StringArgGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import edu.umd.cs.findbugs.annotations.Nullable;
import models.internal.AlertQuery;
import models.internal.MetricsQuery;
import models.internal.MetricsQueryFormat;
import models.internal.Organization;
import models.internal.QueryResult;
import models.internal.alerts.Alert;
import models.internal.impl.DefaultAlert;
import models.internal.impl.DefaultAlertQuery;
import models.internal.impl.DefaultMetricsQuery;
import models.internal.impl.DefaultOrganization;
import models.internal.impl.DefaultQueryResult;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNegative;
import net.sf.oval.constraint.NotNull;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * An alert repository that loads definitions from the filesystem.
 *
 * @author Christian Briones (cbriones at dropbox dot com).
 * @apiNote
 * This repository is read-only, so any additions, deletions, or updates will
 * result in an {@link UnsupportedOperationException}.
 * @implNote
 * This repository is currently tied to the organization given at construction, returning
 * an empty result for any other organization passed in.
 */
public class FileAlertRepository implements AlertRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileAlertRepository.class);
    private final AtomicBoolean _isOpen = new AtomicBoolean(false);
    private final Path _path;
    private final ObjectMapper _objectMapper;
    private final Organization _organization;
    private ImmutableMap<UUID, Alert> _alerts = ImmutableMap.of();

    /**
     * Default Constructor.
     *
     * @param objectMapper The object mapper to use for alert deserialization.
     * @param path The file path for the alert definitions.
     * @param org The organization to group the alerts under.
     */
    @Inject
    public FileAlertRepository(
            final ObjectMapper objectMapper,
            final Path path,
            final UUID org
    ) {
        _objectMapper = objectMapper;
        _path = path;
        _organization = new DefaultOrganization.Builder().setId(org).build();
    }

    @Override
    public void open() {
        assertIsOpen(false);
        LOGGER.debug().setMessage("Opening FileSystemAlertRepository").log();
        _alerts = loadAlerts();
        _isOpen.set(true);
    }

    @Override
    public void close() {
        assertIsOpen();
        LOGGER.debug().setMessage("Closing FileSystemAlertRepository").log();
        _isOpen.set(false);
    }

    @Override
    public Optional<Alert> getAlert(final UUID identifier, final Organization organization) {
        assertIsOpen();
        if (!_organization.equals(organization)) {
            return Optional.empty();
        }
        return Optional.ofNullable(_alerts.get(identifier));
    }

    @Override
    public AlertQuery createAlertQuery(final Organization organization) {
        assertIsOpen();
        return new DefaultAlertQuery(this, organization);
    }

    @Override
    public QueryResult<Alert> queryAlerts(final AlertQuery query) {
        assertIsOpen();

        if (!query.getOrganization().equals(_organization)) {
            return new DefaultQueryResult<>(ImmutableList.of(), 0);
        }

        final Predicate<Alert> containsPredicate =
                query.getContains()
                        .map(c -> (Predicate<Alert>) a -> a.getDescription().contains(c))
                        .orElse(e -> true);

        final ImmutableList<Alert> alerts = _alerts.values().stream()
                .filter(containsPredicate)
                .skip(query.getOffset().orElse(0))
                .limit(query.getLimit())
                .collect(ImmutableList.toImmutableList());

        final long total = _alerts.values().stream()
                .filter(containsPredicate)
                .count();

        return new DefaultQueryResult<>(alerts, total);
    }

    @Override
    public long getAlertCount(final Organization organization) {
        assertIsOpen();
        if (!_organization.equals(organization)) {
            return 0;
        }
        return _alerts.size();
    }

    /* Unsupported mutation operations */

    @Override
    public int deleteAlert(final UUID identifier, final Organization organization) {
        // Since we expect to use this repository just as every other AlertRepository,
        // we should enforce the open-before-use invariant rather than immediately
        // throwing on mutations.
        assertIsOpen();
        throw new UnsupportedOperationException("FilesystemAlertRepository is read-only");
    }

    @Override
    public void addOrUpdateAlert(final Alert alert, final Organization organization) {
        assertIsOpen();
        throw new UnsupportedOperationException("FilesystemAlertRepository is read-only");
    }

    private ImmutableMap<UUID, Alert> loadAlerts() {
        final AlertGroup group;
        try (Reader reader = Files.newBufferedReader(_path)) {
            group = _objectMapper.readValue(
                    reader,
                    AlertGroup.class);
        } catch (final IOException e) {
            throw new RuntimeException("Could not load alerts", e);
        }
        final ImmutableMap.Builder<UUID, Alert> mapBuilder = ImmutableMap.builder();
        for (final SerializedAlert fsAlert : group.getAlerts()) {

            final StringArgGenerator uuidGen = Generators.nameBasedGenerator(_organization.getId());
            final UUID uuid = fsAlert.getUUID().orElseGet(() -> computeUUID(uuidGen, fsAlert));

            // Version-specific attributes.
            //
            // Version 0
            //    query - Queries are KairosDB JSON requests.

            final MetricsQuery query;
            if (group.getVersion() == 0) {
                query = new DefaultMetricsQuery.Builder()
                        .setQuery(fsAlert.getQuery().toString())
                        .setFormat(MetricsQueryFormat.KAIROS_DB)
                        .build();
            } else {
                throw new IllegalArgumentException(String.format("Unhandled alert version %d", group.getVersion()));
            }

            final Alert alert =
                    new DefaultAlert.Builder()
                            .setId(uuid)
                            .setName(fsAlert.getName())
                            .setDescription(fsAlert.getDescription())
                            .setEnabled(fsAlert.isEnabled())
                            .setOrganization(_organization)
                            .setQuery(query)
                            .setAdditionalMetadata(fsAlert.getAdditionalMetadata())
                            .build();
            mapBuilder.put(uuid, alert);
        }
        return mapBuilder.build();
    }

    private UUID computeUUID(final StringArgGenerator uuidGen, final SerializedAlert alert) {
        final String alertContents = alert.getName();
        return uuidGen.generate(alertContents.getBytes(Charset.defaultCharset()));
    }

    private void assertIsOpen() {
        assertIsOpen(true);
    }

    private void assertIsOpen(final boolean expectedState) {
        if (_isOpen.get() != expectedState) {
            throw new IllegalStateException(String.format("FileSystemAlertRepository is not %s",
                    expectedState ? "open" : "closed"));
        }
    }

    private static final class AlertGroup {
        private final List<SerializedAlert> _alerts;
        private final long _version;

        private AlertGroup(final Builder builder) {
            _alerts = builder._alerts;
            _version = builder._version;
        }

        public long getVersion() {
            return _version;
        }

        public List<SerializedAlert> getAlerts() {
            return _alerts;
        }

        private static final class Builder extends OvalBuilder<AlertGroup> {
            private List<SerializedAlert> _alerts;
            @NotNull
            @NotNegative
            private Long _version;

            /**
             * Default constructor.
             * <p>
             * Invoked by Jackson.
             */
            Builder() {
                super(AlertGroup::new);
                _alerts = ImmutableList.of();
            }

            public Builder setAlerts(final List<SerializedAlert> alerts) {
                _alerts = alerts;
                return this;
            }

            public Builder setVersion(final long version) {
                _version = version;
                return this;
            }
        }
    }

    /**
     * The alert data model for this repository.
     */
    private static final class SerializedAlert {
        private final String _name;
        private final String _description;
        private final JsonNode _query;
        private final boolean _enabled;
        private final ImmutableMap<String, Object> _additionalMetadata;
        private final Optional<UUID> _uuid;

        private SerializedAlert(final Builder builder) {
            assert builder._enabled != null;
            assert builder._description != null;
            assert builder._query != null;

            _uuid = Optional.ofNullable(builder._uuid);
            _name = builder._name;
            _description = builder._description;
            _query = builder._query;
            _enabled = builder._enabled;
            _additionalMetadata = builder._additionalMetadata;
        }

        public Optional<UUID> getUUID() {
            return _uuid;
        }

        public String getName() {
            return _name;
        }

        public String getDescription() {
            return _description;
        }

        public JsonNode getQuery() {
            return _query;
        }

        public boolean isEnabled() {
            return _enabled;
        }

        public ImmutableMap<String, Object> getAdditionalMetadata() {
            return _additionalMetadata;
        }

        private static final class Builder extends OvalBuilder<SerializedAlert> {
            @Nullable
            private UUID _uuid;

            @NotNull
            @NotEmpty
            private String _name;

            @NotNull
            @NotEmpty
            private String _description;

            @NotNull
            @NotEmpty
            private JsonNode _query;

            @NotNull
            @Nullable
            private Boolean _enabled;

            private ImmutableMap<String, Object> _additionalMetadata = ImmutableMap.of();

            Builder() {
                super(SerializedAlert::new);
            }

            /**
             * Sets the uuid. If not present, one will be computed using the contents of the alert.
             *
             * @param uuid the uuid.
             * @return This instance of {@code Builder} for chaining.
             */
            public Builder setUuid(@Nullable final UUID uuid) {
                _uuid = uuid;
                return this;
            }

            /**
             * Sets the name. Required. Cannot be empty.
             *
             * @param name the name.
             * @return This instance of {@code Builder} for chaining.
             */
            public Builder setName(final String name) {
                _name = name;
                return this;
            }

            /**
             * Sets the description. Required.
             *
             * @param description the description.
             * @return This instance of {@code Builder} for chaining.
             */
            public Builder setDescription(final String description) {
                _description = description;
                return this;
            }

            /**
             * Sets the query. Required.
             *
             * @param query the query.
             * @return This instance of {@code Builder} for chaining.
             */
            public Builder setQuery(final JsonNode query) {
                _query = query;
                return this;
            }

            /**
             * Sets enabled. Required.
             *
             * @param enabled if this alert is enabled.
             * @return This instance of {@code Builder} for chaining.
             */
            public Builder setEnabled(final boolean enabled) {
                _enabled = enabled;
                return this;
            }

            /**
             * Sets the additional metadata. Defaults to empty.
             *
             * @param additionalMetadata the additional metadata.
             * @return This instance of {@code Builder} for chaining.
             */
            public Builder setAdditionalMetadata(final Map<String, Object> additionalMetadata) {
                _additionalMetadata = ImmutableMap.copyOf(additionalMetadata);
                return this;
            }
        }

    }
}
