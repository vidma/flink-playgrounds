package org.apache.flink.integration.kensu;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BinaryOperator;

import static org.apache.flink.integration.kensu.KensuFlinkHook.logInfo;

public class ReflectHelpers<T> {
    T reflectGetField(Object o, String f) {
        try {
            // getDeclaredField(fieldName) do not always work well...
            Optional<Field> oField = Arrays
                    .stream(o.getClass().getDeclaredFields())
                    .filter((aField) -> aField.getName().equals(f))
                    .findFirst();
            if (oField.isPresent()){
                Field field = oField.get();
                field.setAccessible(true);
                return (T)field.get(o);
            } else {
                // throw new IllegalStateException
                String oExistingFieldList =  Arrays
                        .stream(o.getClass().getDeclaredFields()).map( (aF) -> aF.getName().toString()).reduce("", new BinaryOperator<String>() {
                            @Override
                            public String apply(String s, String s2) {
                                return s + ", " + s2;
                            }
                        });
                logInfo(String.format("field %s do not exist in %s, existingFields: %s", f, o.toString(), oExistingFieldList));
            }
        }  catch (IllegalAccessException | IllegalArgumentException e) {
            //throw new IllegalStateException(
            logInfo(String.format("field %s is not accesible in %s [i.e. IllegalAccessException]", f, o.toString()));
        }
        return null;
    }
}
