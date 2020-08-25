/*
 * Copyright 2010-2020 Boxfuse GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.database.bigquery;

import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Connection;

import java.sql.SQLException;

/**
 * MySQL connection.
 */
public class BigQueryConnection extends Connection<BigQueryDatabase> {
    private static final Log LOG = LogFactory.getLog(BigQueryConnection.class);

    BigQueryConnection(BigQueryDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected void doRestoreOriginalState() throws SQLException {
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return "default";
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) {

    }

    @Override
    protected BigQuerySchema doGetCurrentSchema() throws SQLException {
        String schemaName = getCurrentSchemaNameOrSearchPath();

        // #2206: MySQL and MariaDB can have URLs where no current schema is set, so we must handle this case explicitly.
        return schemaName == null ? null : getSchema(schemaName);
    }

    @Override
    public BigQuerySchema getSchema(String name) {
        return new BigQuerySchema(jdbcTemplate, database, name);
    }
}