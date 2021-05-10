package com.groma.source.sap;

import com.groma.db.TypeCodes;

class DataTypeUtil {
    /*
    -- Mapping of the ABAP Dictionary and ABAP Processor Data Types
    ACCP        6          Accounting period                               n(6)
    CHAR        1-1333     Character string                                c(m)
    CLNT        3          Client                                          c(3)
    CUKY        5          Currency key                                    c(5)
    CURR        1-31       Currency field                                  p((m+1)/2)
    DATS        8          Date                                            d
    DEC         1-31       Calculation/amount field                        p((m+1)/2)
    DF16_RAW    16         Normalized decimal floating point number        decfloat16
    DF16_SCL    16         Scaled decimal floating point number            decfloat16
    DF34_RAW    34         Normalized decimal floating point number        decfloat34
    DF34_SCL    34         Scaled decimal floating point number            decfloat34
    FLTP        16         Floating point number                           f(8)
    INT1        3          1 byte integer                                  b
    INT2        5          2 byte integer                                  s
    INT4        10         4 byte integer                                  i
    LANG        1          Language                                        c(1)
    LCHR        256-…      Long character string                           c(m)
    LRAW        256-…      Long byte string                                x(m)
    NUMC        1-255      Numeric text                                    n(m)
    PREC        2          Obsolete data type                              s
    QUAN        1-31       Quantity field                                  P((m+1)/2)
    RAW         1-255      Byte sequence                                   x(m)
    RAWSTRING   256-…      Byte sequence                                   xstring
    SSTRING     1-255      Character string                                string
    STRING      256-…      Character string                                string
    TIMS        6          Time                                            t
    UNIT        2-3        Unit key                                        c(m)
    */

    /*
    -- Data Type in ABAP Dictionary -> SAP HANA Data Type ->
    CHAR    NVARCHAR
    CLNT    NVARCHAR
    CUKY    NVARCHAR
    LANG    NVARCHAR
    SSTR    NVARCHAR
    STRG    NVARCHAR
    UNIT    NVARCHAR
    VARC    NVARCHAR
    ACCP    VARCHAR
    NUMC    VARCHAR
    DATS    VARCHAR
    TIMS    VARCHAR
    LCHR    NCLOB
    D16D    DECIMAL
    D34D    DECIMAL
    CURR    DECIMAL
    QUAN    DECIMAL
    DEC     DECIMAL
    PREC    DECIMAL
    D16R    VARBINARY
    D16S    VARBINARY
    D34R    VARBINARY
    D34S    VARBINARY
    RAW     VARBINARY
    FLTP    DOUBLE
    INT1    SMALLINT
    INT2    SMALLINT
    INT4    INTEGER
    LRAW    BLOB
    RSTR    BLOB
    */

    /*
    -- ABAP Data Type -> SAP HANA Data Type
    F       DOUBLE
    P       DECIMAL
    D       VARCHAR
    T       VARCHAR
    N       VARCHAR
    C       NVARCHAR
    */

    public static DataTypeInfo getDataType(String typeName, DataTypeInfo info) {
        // typeName: predefined ABAP data type name.
        if (typeName.equals("ACCP")) {
            info.TypeName = "ACCP";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.INTEGER;
            info.Precision = 6;
            info.Scale = 0;
        } else if (typeName.equals("CHAR")) {
            info.TypeName = "CHAR";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.VARCHAR;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("CLNT")) {
            info.TypeName = "CLNT";
            info.ColumnSize = 3;
            info.TypeCode = TypeCodes.VARCHAR;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("CUKY")) {
            info.TypeName = "CUKY";
            info.ColumnSize = 5;
            info.TypeCode = TypeCodes.VARCHAR;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("CURR")) {
            // TODO: test.
            info.TypeName = "CURR";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.DECIMAL;
            info.Precision = -1;
            info.Scale = 2;
        } else if (typeName.equals("DATS")) {
            info.TypeName = "DATS";
            info.ColumnSize = 8;
            info.TypeCode = TypeCodes.DATE;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("DEC")) {
            // TODO: test.
            info.TypeName = "DEC";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.DECIMAL;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("DF16_RAW")) {
            info.TypeName = "DF16_RAW";
            info.ColumnSize = 16;
            info.TypeCode = TypeCodes.VARBINARY;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("DF16_SCL")) {
            info.TypeName = "DF16_SCL";
            info.ColumnSize = 16;
            info.TypeCode = TypeCodes.VARBINARY;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("DF34_RAW")) {
            info.TypeName = "DF34_RAW";
            info.ColumnSize = 34;
            info.TypeCode = TypeCodes.VARBINARY;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("DF34_SCL")) {
            info.TypeName = "DF34_SCL";
            info.ColumnSize = 34;
            info.TypeCode = TypeCodes.VARBINARY;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("FLTP")) {
            info.TypeName = "FLTP";
            info.ColumnSize = 8;
            info.TypeCode = TypeCodes.DOUBLE;
            info.Precision = 16;
            info.Scale = 0;
        } else if (typeName.equals("INT1")) {
            info.TypeName = "INT1";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.TINYINT;
            info.Precision = 3;
            info.Scale = 0;
        } else if (typeName.equals("INT2")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.SMALLINT;
            info.Precision = 5;
            info.Scale = 0;
        } else if (typeName.equals("INT4")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.INTEGER;
            info.Precision = 10;
            info.Scale = 0;
        } else if (typeName.equals("LANG")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("LCHR")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("LRAW")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("NUMC")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("PREC")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("QUAN")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("RAW")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("RAWSTRING")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("SSTRING")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("STRING")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("TIMS")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        } else if (typeName.equals("UNIT")) {
            info.TypeName = "";
            info.ColumnSize = -1;
            info.TypeCode = TypeCodes.XX;
            info.Precision = 0;
            info.Scale = 0;
        }

        return info;
    }

    public static boolean isNumericDataType(int typeCode) {
        return (TypeCodes.TINYINT == typeCode
                || TypeCodes.SMALLINT == typeCode
                || TypeCodes.INTEGER == typeCode
                || TypeCodes.BIGINT == typeCode
                || TypeCodes.FLOAT == typeCode
                || TypeCodes.DOUBLE == typeCode
                || TypeCodes.DECIMAL == typeCode
        );
    }
}

class DataTypeInfo {

    public short TypeCode;

    public int ColumnSize;

    public byte Precision;

    public byte Scale;

    public String TypeName;

    public void DataTypePackage() {
        this.reset();
    }

    public void reset() {
        this.TypeCode = -1;
        this.ColumnSize = -1;
        this.Precision = -1;
        this.Scale = -1;
    }
}