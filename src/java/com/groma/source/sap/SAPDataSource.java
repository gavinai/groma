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
        DataTypePackage typePackage = new DataTypePackage();
        for (int i = 0; i < rowCount; i++) {
            dfies.setRow(i);
            typePackage.reset();
            Metadata.getDataType(dfies.getString("DATATYPE"), typePackage);
            if (typePackage.ColumnSize == -1) {
                int kind = typePackage.getDataTypeKind();
                switch (kind) {
                    case DataTypePackage.KIND_NUMBER:
                        typePackage.ColumnSize = dfies.getInt("INTTYPE");
                        break;
                    default:
                        typePackage.ColumnSize = dfies.getInt("LENG");
                        break;
                }

                typePackage.ColumnSize = (int)dfies.getInt("LENG");
            }

            if (typePackage.Precision == -1) {
                typePackage.Precision = (byte)dfies.getInt("LENG");
            }

            if (typePackage.Scale == -1) {
                typePackage.Scale = (byte)dfies.getInt("DECIMALS");
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
                    typePackage.TypeCode, (short)0, typePackage.ColumnSize, typePackage.Precision, typePackage.Scale, null
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