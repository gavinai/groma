package com.groma;

import com.groma.db.*;
import junit.framework.TestCase;

public final class SAPDataSourceTest extends TestCase {

    public void test_general() throws Exception {
        IDataSource ds = new DataSourceFactory().create("sapds");
        IDataConnection conn = ds.createConnection("Server=10.0.0.89;Password=123456;Client=800;User=ZDDIC;");
        conn.open();
        try {
            Collection<ColumnInfo> columns = ds.getTableColumns(conn, "groma", "sapds", "MARA");
            for (ColumnInfo c : columns) {
                System.out.println(c);
            }
        } finally {
            conn.close();
        }
    }
}