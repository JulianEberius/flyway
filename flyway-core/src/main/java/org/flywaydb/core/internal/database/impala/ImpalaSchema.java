package org.flywaydb.core.internal.database.impala;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ebi on 13.04.16.
 */
public class ImpalaSchema extends Schema<ImpalaDatabase, ImpalaTable> {

    public enum ImpalaTableType {
        TABLE,
        VIEW
    }

    public ImpalaSchema(JdbcTemplate jdbcTemplate, ImpalaDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        List<String> schemas = jdbcTemplate.queryForStringList("show databases");
        return schemas.contains(name);
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        List<String> tables = jdbcTemplate.queryForStringList("show tables in " + database.quote(name));
        return tables.size() == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("create database " + database.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("drop database " + database.quote(name));
    }

    @Override
    protected void doClean() throws SQLException {

        for (String statement : generateDropStatementsForViews()) {
            jdbcTemplate.execute(statement);
        }

        for (ImpalaTable table : allTables()) {
            table.drop();
        }

        for (String statement : generateDropStatementsForFunctions()) {
            jdbcTemplate.execute(statement);
        }

    }

    /**
     * Generates the statements for dropping the views in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> generateDropStatementsForViews() throws SQLException {
        List<String> statements = new ArrayList<String>();
        // since Impala currently does not support any "show views" command (or similar),
        // we ask for tables and filter the views based on the result of a "describe" statement
        List<String> tableNames =
                jdbcTemplate.queryForStringList("show tables in " + database.quote(name));
        for (String tableName: tableNames) {
            if (getTableType(tableName) == ImpalaTableType.VIEW)
                statements.add("drop view if exists " + database.quote(name, tableName));
        }

        return statements;
    }

    /**
     * Generates the statements for dropping the routines in this schema.
     *
     * @return The drop statements.
     * @throws SQLException when the clean statements could not be generated.
     */
    private List<String> generateDropStatementsForFunctions() throws SQLException {
        List<Map<String, String>> rows =
                jdbcTemplate.queryForList("show functions in " + database.quote(name));

        List<String> statements = new ArrayList<String>();
        for (Map<String,String> row: rows) {
            String signature = row.get("signature");
            if (signature != null)
                statements.add("drop function if exists " + database.quote(name) + "." + signature);
        }
        return statements;
    }

    @Override
    protected ImpalaTable[] doAllTables() throws SQLException {
        List<String> tableNames =
                jdbcTemplate.queryForStringList("show tables in " + database.quote(name));

        // exclude views by explicitely asking for their table type, as Impala sadly returns Views
        // in its "show tables" result
        List<ImpalaTable> tables = new ArrayList<ImpalaTable>();
        for (String tableName: tableNames) {
            if (getTableType(tableName) == ImpalaTableType.TABLE)
                tables.add(new ImpalaTable(jdbcTemplate, database, this, tableName));
        }
        return tables.toArray(new ImpalaTable[tables.size()]);
    }

    protected ImpalaTableType getTableType(String tableName) throws SQLException {
        List<Map<String, String>> rows = jdbcTemplate.queryForList("describe formatted " + database.quote(name, tableName));

        for (Map<String, String> row : rows) {
            String key = row.get("name");
            if (key != null && key.startsWith("Table Type")) {
                String value = row.get("type");
                if (value.contains("VIEW"))
                    return ImpalaTableType.VIEW;
                else
                    return ImpalaTableType.TABLE;
            }
        }
        return null;
    }


    @Override
    public ImpalaTable getTable(String tableName) {
        return new ImpalaTable(jdbcTemplate, database, this, tableName);
    }
}
