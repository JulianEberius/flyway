package org.flywaydb.core.internal.database.bigquery;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BigQuerySchema extends Schema<BigQueryDatabase, BigQueryTable> {

    public BigQuerySchema(JdbcTemplate jdbcTemplate, BigQueryDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        List<String> schemas = jdbcTemplate.queryForStringList("SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA");
        return schemas.contains(name);
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        List<String> tables = jdbcTemplate.queryForStringList("SELECT table_name FROM "+ database.quote(name) + ".INFORMATION_SCHEMA.TABLES");
        return tables.size() == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        throw new UnsupportedOperationException("Cannot create BigQuery datasets via SQL");
    }

    @Override
    protected void doDrop() throws SQLException {
        throw new UnsupportedOperationException("Cannot drop BigQuery datasets via SQL");
    }

    @Override
    protected void doClean() throws SQLException {
        throw new UnsupportedOperationException("Cannot clean BigQuery schemata via SQL");
    }

    @Override
    protected BigQueryTable[] doAllTables() throws SQLException {
        List<String> tableNames =
                jdbcTemplate.queryForStringList("SELECT table_name FROM "+ database.quote(name) + ".INFORMATION_SCHEMA.TABLES");

        List<BigQueryTable> tables = new ArrayList<BigQueryTable>();
        for (String tableName: tableNames) {
            tables.add(new BigQueryTable(jdbcTemplate, database, this, tableName));
        }
        return tables.toArray(new BigQueryTable[tables.size()]);
    }

    @Override
    public BigQueryTable getTable(String tableName) {
        return new BigQueryTable(jdbcTemplate, database, this, tableName);
    }
}
