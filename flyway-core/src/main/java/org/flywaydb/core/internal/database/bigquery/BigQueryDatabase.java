package org.flywaydb.core.internal.database.bigquery;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.Connection;

public class BigQueryDatabase extends Database<BigQueryConnection> {

    private final JdbcTemplate jdbcTemplate;

    public BigQueryDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory) {
        super(configuration, jdbcConnectionFactory);
        jdbcTemplate = new JdbcTemplate(rawMainJdbcConnection, databaseType);
    }

    @Override
    protected BigQueryConnection doGetConnection(Connection connection) {
        return new BigQueryConnection(this, connection);
    }

    @Override
    public String getSelectStatement(Table table) {
        return super.getSelectStatement(table);
    }

    @Override
    public void ensureSupported() {

    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public boolean supportsChangingCurrentSchema() {
        return true;
    }

    @Override
    public String getBooleanTrue() {
        return "true";
    }

    @Override
    public String getBooleanFalse() {
        return "false";
    }

    @Override
    protected String doQuote(String identifier) {
        return String.format("`%s`", identifier);
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        return "CREATE TABLE " + table + " (\n" +
                "    `installed_rank` INT64,\n" +
                "    `version` STRING,\n" +
                "    `description` STRING,\n" +
                "    `type` STRING,\n" +
                "    `script` STRING,\n" +
                "    `checksum` INT64,\n" +
                "    `installed_by` STRING,\n" +
                "    `installed_on` TIMESTAMP,\n" +
                "    `execution_time` INT64,\n" +
                "    `success` BOOL\n" +
                ");\n" +
                (baseline ? getBaselineStatement(table) + ";\n" : "");
    }


    @Override
    public String getInsertStatement(Table table) {
        return "INSERT INTO " + table
                + " (" + quote("installed_rank")
                + ", " + quote("version")
                + ", " + quote("description")
                + ", " + quote("type")
                + ", " + quote("script")
                + ", " + quote("checksum")
                + ", " + quote("installed_by")
                + ", " + quote("installed_on")
                + ", " + quote("execution_time")
                + ", " + quote("success")
                + ")"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp(), ?, ?)";
    }

    @Override
    public boolean supportsMultiStatementTransactions() {
        return false;
    }
}
