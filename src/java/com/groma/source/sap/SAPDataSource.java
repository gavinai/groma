package com.groma.source.sap;

import com.groma.db.*;
import com.sap.conn.jco.*;
import com.sap.conn.jco.ext.Environment;

public final class SAPDataSource implements IDataSource {

    private String DEFAULT_CATALOG_NAME = "Groma";

    private String DEFAULT_SCHEMA_NAME = "Sapds";

    public SAPDataSource() {
        ;
    }

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
        if (catalogName == null || schemaName == null || tableName == null) {
            throw new Exception("The CatalogName, SchemaName and TableName are required.");
        }

        if (!DEFAULT_CATALOG_NAME.equalsIgnoreCase(catalogName) || !DEFAULT_SCHEMA_NAME.equalsIgnoreCase(schemaName)) {
            return new Collection<ColumnInfo>(0);
        }

        JCoDestination dest = ((SAPDataConnection)conn).getDestination();
        if (dest == null) {
            throw new Exception("Unable to get the valid destination. ");
        }

        JCoRepository repo = dest.getRepository();
        if (repo == null) {
            throw new Exception("Unable to get the valid repository. ");
        }

        JCoFunction func = repo.getFunction("DDIF_FIELDINFO_GET");
        if (repo == null) {
            throw new Exception("Unable to get the 'DDIF_FIELDINFO_GET' function. ");
        }

        JCoParameterList imports = func.getImportParameterList();
        imports.setValue("TABNAME", tableName);
        JCoContext.begin(dest);
        try {
            func.execute(dest);
        } catch (Exception ex) {
            throw new Exception("Fails to execute the 'DDIF_FIELDINFO_GET' function. ", ex);
        } finally {
            try {
                JCoContext.end(dest);
            } catch (Exception ex) {
                ;
            }
        }

        JCoParameterList tables = func.getTableParameterList();
        JCoTable dfies = tables.getTable("DFIES_TAB");
        int rowCount = dfies.getNumRows();
        Collection<ColumnInfo> columns = new Collection<ColumnInfo>(rowCount);
        DataTypeInfo typeInfo = new DataTypeInfo();
        for (int i = 0; i < rowCount; i++) {
            dfies.setRow(i);
            typeInfo.reset();
            DataTypeUtil.getDataType(dfies.getString("DATATYPE"), typeInfo);
            if (typeInfo.ColumnSize == -1) {
                if (DataTypeUtil.isNumericDataType(typeInfo.TypeCode)) {
                    typeInfo.ColumnSize = dfies.getInt("INTTYPE");
                } else {
                    typeInfo.ColumnSize = dfies.getInt("LENG");
                }
            }

            if (typeInfo.Precision == -1) {
                typeInfo.Precision = (byte)dfies.getInt("LENG");
            }

            if (typeInfo.Scale == -1) {
                typeInfo.Scale = (byte)dfies.getInt("DECIMALS");
            }

            Object DATATYPE = dfies.getValue("DATATYPE");
            Object LENG = dfies.getValue("LENG");
            Object DECIMALS = dfies.getValue("DECIMALS");
            Object INTTYPE = dfies.getValue("INTTYPE");
            Object INTLEN = dfies.getValue("INTLEN");
            Object FIELDNAME = dfies.getValue("FIELDNAME");
//            short typeCode = Metadata.getTypeCode(dfies.getString("DATATYPE"));
//            int columnSize;
//            switch (typeCode) {
//                case TypeCode.VARCHAR:
//                    columnSize = dfies.getInt("LENG");
//                    break;
//            }

            ColumnInfo info = new ColumnInfo(
                    dfies.getString("TABNAME"), dfies.getString("FIELDNAME"),
                    typeInfo.TypeCode, (short)0, typeInfo.ColumnSize, typeInfo.Precision, typeInfo.Scale, null
            );

            columns.add(info);
        }

        return columns;

        //imports.setValue("ALL_TYPES", "");

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