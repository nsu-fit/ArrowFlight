package net.surpin.data.arrowflight.client.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FieldVectorTest {
    @Test
    void getFieldReturnsConstructedField() {
        FieldVector fv = new FieldVector("test_field", new FieldType(FieldType.IDs.INT), new Object[]{1, 2, 3});
        Field field = fv.getField();
        assertEquals("test_field", field.getName());
        assertEquals(FieldType.IDs.INT, field.getType().getTypeID());
    }

    @Test
    void getValuesReturnsPassedArray() {
        Object[] values = new Object[]{"a", "b", "c"};
        FieldVector fv = new FieldVector("str_field", new FieldType(FieldType.IDs.VARCHAR), values);
        assertArrayEquals(values, fv.getValues());
    }

    @Test
    void getValuesForEmptyArray() {
        FieldVector fv = new FieldVector("empty", new FieldType(FieldType.IDs.INT), new Object[0]);
        assertEquals(0, fv.getValues().length);
    }
}
