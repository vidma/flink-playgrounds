package org.apache.flink.integration.kensu.reflect;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.BinaryOperator;

import static org.apache.flink.integration.kensu.KensuFlinkHook.logInfo;

public class ReflectHelpers<T> {
    // https://stackoverflow.com/a/54487633
    // getFields(): Gets all the public fields up the entire class hierarchy and
    // getDeclaredFields(): Gets all the fields, regardless of their modifiers **but only for the current class**.
    // So, you have to get for all the hierarchy involved.
    public static List<Field> getAllFieldsList(final Class<?> cls) {
        //Validate.isTrue(cls != null, "The class must not be null");
        final List<Field> allFields = new ArrayList<>();
        final Set<String> addedFields = new HashSet<>();
        Class<?> currentClass = cls;
        while (currentClass != null) {
            for (Field field : currentClass.getDeclaredFields()) {
                String fieldName = field.getName();
                if (!addedFields.contains(fieldName)){
                    addedFields.add(fieldName);
                    allFields.add(field);
                }
            }
            currentClass = currentClass.getSuperclass();
        }
        return allFields;
    }

    public T reflectGetField(Object o, String f) {
        try {
            List<Field> allClassFields = getAllFieldsList(o.getClass()); // including superclasses
            Optional<Field> oField = allClassFields
                    .stream()
                    .filter((aField) -> aField.getName().equals(f))
                    .findFirst();
            if (oField.isPresent()){
                Field field = oField.get();
                field.setAccessible(true);
                return (T)field.get(o);
            } else {
                // throw new IllegalStateException
                String oExistingFieldList = concatFieldList(allClassFields);
                logInfo(String.format("field %s do not exist in %s, existingFields: %s", f, o.toString(), oExistingFieldList));
            }
        }  catch (IllegalAccessException | IllegalArgumentException e) {
            //throw new IllegalStateException(
            logInfo(String.format("field %s is not accesible in %s [i.e. IllegalAccessException]", f, o.toString()));
        }
        return null;
    }

    private String concatFieldList(List<Field> allClassFields) {
        return allClassFields
                .stream()
                .map( (aF) -> aF.getName().toString())
                .reduce("", (s, s2) -> s + ", " + s2);
    }
}
