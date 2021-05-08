package com.groma.source.sap;

import com.groma.db.*;
import com.sap.conn.jco.*;
import com.sap.conn.jco.ext.Environment;

public final class SAPDataSource implements IDataSource {

    @Override
    public String getName() {
        return "sapds";
    }

    @Override
    public IDataConnection createConnection(String connectionString) throws Exception {
        if (!Environment.isDestinationDataProviderRegistered()) {
            Environment.registerDestinationDataProvider(SAPConnectionDataManager.INSTANCE);
        }

        return new SAPDataConnection(connectionString);
    }

    @Override
    public Collection<ColumnInfo> getTableColumns(IDataConnection conn, String catalogName, String schemaName, String tableName) throws Exception {
        JCoDestination dest = ((SAPDataConnection)conn).getDestination();
        JCoRepository repo = dest.getRepository();
        if (repo == null) {
            throw new Exception("Unable to get the valid repository. ");
        }

        JCoFunction func = repo.getFunction("DDIF_FIELDINFO_GET");
        if (repo == null) {
            throw new Exception("Unable to get the 'DDIF_FIELDINFO_GET' function. ");
        }

        return null;
//
//        JCoParameterList exportParameters = funcRfcReadTable.getExportParameterList();
//        JCoParameterList importParameters = funcRfcReadTable.getImportParameterList();
//        JCoParameterList tableParameters = funcRfcReadTable.getTableParameterList();
//
//        Trace.STDOUT.println("==================== Original DDIF_FIELDINFO_GET Info =========================");
//        Trace.STDOUT.print(funcRfcReadTable);
//
//        importParameters.setValue("ALL_TYPES", "");
//        importParameters.setValue("DO_NOT_WRITE", "Write");
//        importParameters.setValue("FIELDNAME", "");
//        importParameters.setValue("GROUP_NAMES", "");
//        importParameters.setValue("LANGU", "");
//        importParameters.setValue("LFIELDNAME", "");
//        importParameters.setValue("TABNAME", "MDMA");
//        importParameters.setValue("UCLEN", "");// ????? not sure what's this for. leave it as blank, it will be generated automatically.
//
//        Trace.STDOUT.println("==================== Request DDIF_FIELDINFO_GET Info =========================");
//        Trace.STDOUT.print(funcRfcReadTable);
//
//        funcRfcReadTable.execute(destination);
//
//        Trace.STDOUT.println("==================== Response DDIF_FIELDINFO_GET Info =========================");
//        Trace.STDOUT.print(funcRfcReadTable);
    }
}