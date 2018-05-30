package datamodel;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ApixMsgStructure {
    public static final String QSVERSION_ID = "qsversionid";
    public static final String UUID = "uuid";
    public static final String FEINGENRE = "feingenre";
    public static final String BEITRAGMGRID = "beitragmrgid";

    public StructType getSchema(){
        return new StructType()
                .add(ApixMsgStructure.QSVERSION_ID,DataTypes.StringType)
                .add(ApixMsgStructure.UUID,DataTypes.StringType)
                .add(ApixMsgStructure.FEINGENRE,DataTypes.StringType)
                .add(ApixMsgStructure.BEITRAGMGRID,DataTypes.StringType);
    }
}
