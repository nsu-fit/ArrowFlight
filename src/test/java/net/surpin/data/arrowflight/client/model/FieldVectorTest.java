package net.surpin.data.arrowflight.client.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FieldVectorTest {

    @Test
    void constructorAndGetters() {
        FieldType type = new FieldType(FieldType.IDs.DOUBLE);
        Object[] values = { 1.0, 2.0, 3.0 };

        FieldVector fv = new FieldVector("score", type, values);

        assertEquals("score", fv.getField().getName());
        assertEquals(FieldType.IDs.DOUBLE, fv.getField().getType().getTypeID());
        assertArrayEquals(values, fv.getValues());
    }

    @Test
    void emptyValues() {
        FieldVector fv = new FieldVector("flag", new FieldType(FieldType.IDs.BOOLEAN), new Object[0]);

        assertEquals(0, fv.getValues().length);
        assertEquals(FieldType.IDs.BOOLEAN, fv.getField().getType().getTypeID());
    }

    @Test
    void nullValuesArrayPreserved() {
        FieldVector fv = new FieldVector("col", new FieldType(FieldType.IDs.VARCHAR), null);

        assertNull(fv.getValues());
    }

    @Test
    void mixedTypeValues() {
        Object[] values = { 1, "two", true };
        FieldVector fv = new FieldVector("mixed", new FieldType(FieldType.IDs.VARCHAR), values);

        assertEquals(3, fv.getValues().length);
        assertEquals(1, fv.getValues()[0]);
        assertEquals("two", fv.getValues()[1]);
        assertEquals(true, fv.getValues()[2]);
    }
}
