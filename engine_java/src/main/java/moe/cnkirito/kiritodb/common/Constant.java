package moe.cnkirito.kiritodb.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

public class Constant {

    public static final String DataName = "/data";
    public static final String DataSuffix = ".polar";
    public static final String IndexName = "/index";
    public static final String IndexSuffix = ".polar";
    public static final int ValueLength = 4 * 1024;
    public static final int IndexLength = 8;
    public static final EngineException ioException = new EngineException(RetCodeEnum.IO_ERROR, "io exception");
    public static final EngineException keyNotFoundException = new EngineException(RetCodeEnum.NOT_FOUND, "key not found");


}
