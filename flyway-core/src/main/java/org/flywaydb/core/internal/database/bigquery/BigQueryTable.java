/**
 * Copyright 2010-2016
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
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class BigQueryTable extends Table<BigQueryDatabase, BigQuerySchema> {
    private static final Log LOG = LogFactory.getLog(BigQueryTable.class);

    BigQueryTable(JdbcTemplate jdbcTemplate, BigQueryDatabase database, BigQuerySchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        throw new UnsupportedOperationException("Cannot drop BigQuery tables via SQL");
    }

    @Override
    protected boolean doExists() throws SQLException {
        return exists(null, schema, name);
    }

    @Override
    protected void doLock() throws SQLException {
        LOG.debug("Unable to lock " + this + " as BigQuery does not support locking. Concurrent migrations are not supported.");
    }
}

