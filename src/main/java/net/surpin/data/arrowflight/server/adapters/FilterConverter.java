package net.surpin.data.arrowflight.server.adapters;

import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.CallConverter;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SqlExpressionToSubstrait;
import io.substrait.isthmus.expression.CallConverters;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Converts SQL filter expressions to Substrait ByteBuffers for Arrow Acero filter pushdown.
 * Works around isthmus 0.91 CAST, n-ary AND/OR, and nullable-type limitations.
 */
public final class FilterConverter {

    private static final ConverterProvider CAST_AWARE_PROVIDER = new ConverterProvider() {
        @Override
        public ScalarFunctionConverter getScalarFunctionConverter() {
            ScalarFunctionConverter base = super.getScalarFunctionConverter();
            CallConverter castConv = CallConverters.CAST.apply(getTypeConverter());
            RexBuilder rexBuilder = new RexBuilder(getTypeFactory());
            return new ScalarFunctionConverter(Collections.emptyList(), getTypeFactory()) {
                @Override
                public Optional<io.substrait.expression.Expression> convert(
                        RexCall call, Function<RexNode, io.substrait.expression.Expression> visitor) {
                    if ((call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR)
                            && call.getOperands().size() > 2) {
                        RexNode folded = call.getOperands().get(0);
                        for (int i = 1; i < call.getOperands().size(); i++) {
                            folded = rexBuilder.makeCall(call.getOperator(), folded, call.getOperands().get(i));
                        }
                        return Optional.of(visitor.apply(folded));
                    }
                    if (call.getKind() == SqlKind.CAST) {
                        Optional<io.substrait.expression.Expression> dateLiteral = nativeDateLiteral(call);
                        if (dateLiteral.isPresent()) {
                            return dateLiteral;
                        }
                        return castConv.convert(call, visitor)
                                .or(() -> Optional.of(visitor.apply(call.getOperands().get(0))));
                    }
                    return castConv.convert(call, visitor).or(() -> base.convert(call, visitor));
                }
            };
        }
    };

    /**
     * Converts a character-to-date cast into a native Substrait date literal.
     *
     * @param call Calcite cast expression
     * @return native date literal when the cast contains an ISO date string
     */
    private static Optional<io.substrait.expression.Expression> nativeDateLiteral(RexCall call) {
        if (call.getType().getSqlTypeName() != SqlTypeName.DATE
                || call.getOperands().size() != 1
                || !(call.getOperands().get(0) instanceof RexLiteral literal)) {
            return Optional.empty();
        }
        try {
            LocalDate date = LocalDate.parse(RexLiteral.stringValue(literal));
            return Optional.of(ExpressionCreator.date(
                    call.getType().isNullable(), Math.toIntExact(date.toEpochDay())));
        } catch (DateTimeParseException | ClassCastException exception) {
            return Optional.empty();
        }
    }

    /**
     * Utility class, no instantiation.
     */
    private FilterConverter() {
    }

    /**
     * Pre-warms the Calcite and Substrait-Java class graph to avoid ~1.5s class-loading cost
     * on the first real conversion.
     *
     * @param strippedCreateStatements DDL statements without schema prefix
     */
    public static void warmUp(List<String> strippedCreateStatements) {
        if (strippedCreateStatements.isEmpty()) {
            return;
        }
        String firstDdl = strippedCreateStatements.get(0);
        String dummyFilter = "1 = 1";
        java.util.regex.Matcher m =
                java.util.regex.Pattern.compile("\"([^\"]+)\"").matcher(firstDdl);
        if (m.find()) {
            dummyFilter = "\"" + m.group(1) + "\" IS NOT NULL";
        }
        try {
            toByteBuffer(dummyFilter, strippedCreateStatements);
        } catch (Exception ignored) {
        }
    }

    /**
     * Converts a SQL WHERE expression to a Substrait ByteBuffer for Acero filter pushdown.
     *
     * @param sqlExpression    SQL expression (no SELECT/WHERE keywords)
     * @param createStatements DDL CREATE TABLE statements defining the schema
     * @return direct ByteBuffer positioned at 0 with serialized ExtendedExpression
     * @throws SqlParseException on parse failure
     */
    public static ByteBuffer toByteBuffer(String sqlExpression, List<String> createStatements)
            throws SqlParseException {
        SqlExpressionToSubstrait converter = new SqlExpressionToSubstrait(CAST_AWARE_PROVIDER);
        byte[] bytes = converter.convert(sqlExpression, createStatements).toByteArray();
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        buffer.rewind();
        return buffer;
    }
}
