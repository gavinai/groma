package com.groma.db;

public interface IDataConnection {
    void open() throws Exception;
    void close() throws Exception;
}
