package com.groma.db;

public interface IDataSource {
    String getName();
    IDataConnection createConnection(String connectionString) throws Exception;
    Collection<ColumnInfo> getTableColumns(IDataConnection conn, String catalogName, String schemaName, String tableName) throws Exception;
}