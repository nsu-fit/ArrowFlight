package net.surpin.data.arrowflight.client.model;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FieldTypeTest {

    @Test
    void baseConstructorAndGetTypeId() {
        for (FieldType.IDs id : FieldType.IDs.values()) {
            FieldType ft = new FieldType(id);
            assertEquals(id, ft.getTypeID());
        }
    }

    // ── DecimalType ─────────────────────────────────────────────────────────

    @Test
    void decimalType() {
        FieldType.DecimalType dt = new FieldType.DecimalType(38, 10);

        assertEquals(FieldType.IDs.DECIMAL, dt.getTypeID());
        assertEquals(38, dt.getPrecision());
        assertEquals(10, dt.getScale());
    }

    // ── BinaryType ──────────────────────────────────────────────────────────

    @Test
    void binaryTypeFixedWidth() {
        FieldType.BinaryType bt = new FieldType.BinaryType(16);

        assertEquals(FieldType.IDs.BYTES, bt.getTypeID());
        assertEquals(16, bt.getByteWidth());
    }

    @Test
    void binaryTypeVariableWidth() {
        FieldType.BinaryType bt = new FieldType.BinaryType(-1);

        assertEquals(-1, bt.getByteWidth());
    }

    // ── ListType ────────────────────────────────────────────────────────────

    @Test
    void listTypeFixedLength() {
        FieldType child = new FieldType(FieldType.IDs.INT);
        FieldType.ListType lt = new FieldType.ListType(10, child);

        assertEquals(FieldType.IDs.LIST, lt.getTypeID());
        assertEquals(10, lt.getLength());
        assertSame(child, lt.getChildType());
    }

    @Test
    void listTypeDynamicLength() {
        FieldType child = new FieldType(FieldType.IDs.VARCHAR);
        FieldType.ListType lt = new FieldType.ListType(child);

        assertEquals(FieldType.IDs.LIST, lt.getTypeID());
        assertEquals(-1, lt.getLength());
        assertSame(child, lt.getChildType());
    }

    // ── MapType ─────────────────────────────────────────────────────────────

    @Test
    void mapType() {
        FieldType keyType = new FieldType(FieldType.IDs.VARCHAR);
        FieldType valueType = new FieldType(FieldType.IDs.INT);
        FieldType.MapType mt = new FieldType.MapType(keyType, valueType);

        assertEquals(FieldType.IDs.MAP, mt.getTypeID());
        assertSame(keyType, mt.getKeyType());
        assertSame(valueType, mt.getValueType());
    }

    // ── StructType ──────────────────────────────────────────────────────────

    @Test
    void structType() {
        FieldType intType = new FieldType(FieldType.IDs.INT);
        FieldType strType = new FieldType(FieldType.IDs.VARCHAR);
        Map<String, FieldType> children = Map.of("id", intType, "name", strType);

        FieldType.StructType st = new FieldType.StructType(children);

        assertEquals(FieldType.IDs.STRUCT, st.getTypeID());
        assertEquals(2, st.getChildrenType().size());
        assertSame(intType, st.getChildrenType().get("id"));
        assertSame(strType, st.getChildrenType().get("name"));
    }

    // ── UnionType ───────────────────────────────────────────────────────────

    @Test
    void unionType() {
        Map<String, FieldType> children = Map.of("int_val", new FieldType(FieldType.IDs.INT));
        FieldType.UnionType ut = new FieldType.UnionType(children);

        assertEquals(FieldType.IDs.STRUCT, ut.getTypeID());
        assertEquals(1, ut.getChildrenType().size());
    }
}
