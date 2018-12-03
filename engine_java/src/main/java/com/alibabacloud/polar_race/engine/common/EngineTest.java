package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.io.File;

/**
 * @author daofeng.xjf
 * @date 2018/12/3
 */
public class EngineTest {
    public static void main(String[] args) throws EngineException {
        File file = new File("/tmp/kiritoDB");
        deleteDir(file);
        new WriteTest().test();
        new ReadTest().test();
        new RangeTest().test();
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            //递归删除目录中的子目录下
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }

}
