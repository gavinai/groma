package com.groma.db;

public class DataSourceFactory {

    private static IDataSource[] UNKNOWN_DS = new IDataSource[] {
            new com.groma.source.sap.SAPDataSource()
    };
    
    public IDataSource create(String name) throws Exception {
        for (IDataSource ds : UNKNOWN_DS) {
            if (ds.getName().equalsIgnoreCase(name)) {
                return ds;
            }
        }

        throw new Exception("Unable to find the data source '" + name + "'. ");
    }
}
