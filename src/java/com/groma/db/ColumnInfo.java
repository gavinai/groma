package com.groma.db;

public class ColumnInfo {

    public final String TableName, ColumnName;

    public final short TypeCode, SubTypeCode;

    public final int ColumnSize;

    public final byte Precision, Scale;

    public final String DataTypeName;

    public ColumnInfo(String tableName, String columnName, short typeCode, short subTypeCode, int columnSize, byte precision, byte scale, String dataTypeName) {
        this.TableName = tableName;
        this.ColumnName = columnName;
        this.TypeCode = typeCode;
        this.SubTypeCode = subTypeCode;
        this.ColumnSize = columnSize;
        this.Precision = precision;
        this.Scale = scale;
        this.DataTypeName = dataTypeName;
    }

}