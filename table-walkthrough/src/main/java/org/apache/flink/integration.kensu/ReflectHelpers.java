package org.apache.flink.integration.kensu;

import java.lang.reflect.Field;

import static org.apache.flink.integration.kensu.KensuFlinkHook.logInfo;

public class ReflectHelpers<T> {
    T reflectGetField(Object o, String f) {
        try {
            Field field = o.getClass().getDeclaredField(f);
            field.setAccessible(true);
            return (T)field.get(o);
        } catch (NoSuchFieldException e) {
            // throw new IllegalStateException
            logInfo(String.format("field %s do not exist in %s", f, o.toString()));
        } catch (IllegalAccessException e) {
            //throw new IllegalStateException(
            logInfo(String.format("field %s is not accesible in %s [i.e. IllegalAccessException]", f, o.toString()));
        }
        return null;
    }
}
