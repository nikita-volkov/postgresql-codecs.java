package io.pgenie.postgresqlCodecs.codecs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

final class NumericCodec implements Codec<BigDecimal> {

    static final NumericCodec instance = new NumericCodec();

    private NumericCodec() {
    }

    public String name() {
        return "numeric";
    }

    @Override
    public int oid() {
        return 1700;
    }

    @Override
    public int arrayOid() {
        return 1231;
    }

    @Override
    public void bind(PreparedStatement ps, int index, BigDecimal value) throws SQLException {
        if (value != null) {
            ps.setBigDecimal(index, value);
        } else {
            ps.setNull(index, Types.NUMERIC);
        }
    }

    public void write(StringBuilder sb, BigDecimal value) {
        sb.append(value.toPlainString());
    }

    @Override
    public Codec.ParsingResult<BigDecimal> parse(CharSequence input, int offset) throws Codec.ParseException {
        int len = input.length();
        if (offset >= len) {
            throw new Codec.ParseException(input, offset, "Expected numeric, reached end of input");
        }
        // Check for NaN
        if (offset + 3 <= len && input.subSequence(offset, offset + 3).toString().equals("NaN")) {
            throw new Codec.ParseException(input, offset, "NaN is not supported for numeric");
        }
        int i = offset;
        while (i < len && isNumericChar(input.charAt(i))) {
            i++;
        }
        if (i == offset) {
            throw new Codec.ParseException(input, offset, "Expected numeric value");
        }
        String token = input.subSequence(offset, i).toString();
        try {
            BigDecimal value = new BigDecimal(token);
            return new Codec.ParsingResult<>(value, i);
        } catch (NumberFormatException e) {
            throw new Codec.ParseException(input, offset, "Invalid numeric: " + token);
        }
    }

    @Override
    public byte[] encode(java.math.BigDecimal value) {
        int sign;
        java.math.BigDecimal absValue;
        if (value.signum() < 0) {
            sign = 0x4000;
            absValue = value.negate();
        } else {
            sign = 0x0000;
            absValue = value;
        }
        int dscale = Math.max(0, value.scale());

        // Use plain string for digit extraction
        String plain = absValue.toPlainString();
        int dotPos = plain.indexOf('.');
        String intStr = (dotPos < 0) ? plain : plain.substring(0, dotPos);
        String fracStr = (dotPos < 0) ? "" : plain.substring(dotPos + 1);

        // Pad integer part on the left to a multiple of 4
        int intPad = (4 - (intStr.length() % 4)) % 4;
        String intPadded = "0".repeat(intPad) + intStr;

        // Pad fractional part on the right to a multiple of 4
        int fracPad = fracStr.isEmpty() ? 0 : (4 - (fracStr.length() % 4)) % 4;
        String fracPadded = fracStr + "0".repeat(fracPad);

        int nIntGroups = intPadded.length() / 4;
        int nFracGroups = fracPadded.isEmpty() ? 0 : fracPadded.length() / 4;

        // Build list of int16 digit groups
        java.util.List<Short> digits = new java.util.ArrayList<>();

        // Determine the weight and collect integer groups
        int firstNonZeroIntGroup = -1;
        short[] intGroups = new short[nIntGroups];
        for (int i = 0; i < nIntGroups; i++) {
            intGroups[i] = (short) Integer.parseInt(intPadded.substring(i * 4, (i + 1) * 4));
            if (firstNonZeroIntGroup == -1 && intGroups[i] != 0) {
                firstNonZeroIntGroup = i;
            }
        }

        short[] fracGroups = new short[nFracGroups];
        for (int i = 0; i < nFracGroups; i++) {
            fracGroups[i] = (short) Integer.parseInt(fracPadded.substring(i * 4, (i + 1) * 4));
        }

        int weight;
        if (firstNonZeroIntGroup == -1) {
            // Integer part is zero (value < 1 or value == 0)
            int firstNonZeroFracGroup = -1;
            for (int i = 0; i < nFracGroups; i++) {
                if (fracGroups[i] != 0) { firstNonZeroFracGroup = i; break; }
            }
            if (firstNonZeroFracGroup == -1) {
                // value is exactly zero
                java.nio.ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                buf.putShort((short) 0); buf.putShort((short) 0);
                buf.putShort((short) sign); buf.putShort((short) dscale);
                return buf.array();
            }
            weight = -(firstNonZeroFracGroup + 1);
            for (int i = firstNonZeroFracGroup; i < nFracGroups; i++) {
                digits.add(fracGroups[i]);
            }
        } else {
            weight = nIntGroups - 1 - firstNonZeroIntGroup;
            for (int i = firstNonZeroIntGroup; i < nIntGroups; i++) {
                digits.add(intGroups[i]);
            }
            for (int i = 0; i < nFracGroups; i++) {
                digits.add(fracGroups[i]);
            }
        }

        // Remove trailing zero groups
        while (!digits.isEmpty() && digits.get(digits.size() - 1) == 0) {
            digits.remove(digits.size() - 1);
        }

        java.nio.ByteBuffer buf = ByteBuffer.allocate(8 + digits.size() * 2).order(ByteOrder.BIG_ENDIAN);
        buf.putShort((short) digits.size());
        buf.putShort((short) weight);
        buf.putShort((short) sign);
        buf.putShort((short) dscale);
        for (short d : digits) buf.putShort(d);
        return buf.array();
    }

    @Override
    public java.math.BigDecimal decodeBinary(java.nio.ByteBuffer buf, int length) throws Codec.ParseException {
        if (length < 8) throw new Codec.ParseException("Binary numeric too short: " + length);
        short ndigits = buf.getShort();
        short weight = buf.getShort();
        short sign = buf.getShort();
        short dscale = buf.getShort();

        if (sign == (short) 0xC000) {
            throw new Codec.ParseException("NaN is not supported for numeric");
        }

        short[] digits = new short[ndigits];
        for (int i = 0; i < ndigits; i++) {
            digits[i] = buf.getShort();
        }

        if (ndigits == 0) {
            return java.math.BigDecimal.ZERO.setScale(dscale, java.math.RoundingMode.UNNECESSARY);
        }

        // Reconstruct decimal string
        StringBuilder sb = new StringBuilder();
        if (sign == (short) 0x4000) sb.append('-');

        if (weight < 0) {
            // All digits are fractional; insert leading zeros before them
            sb.append("0.");
            for (int i = 0; i < -(weight + 1); i++) sb.append("0000");
            for (int i = 0; i < ndigits; i++) sb.append(String.format("%04d", digits[i]));
        } else {
            // Integer groups: indices 0..weight
            for (int i = 0; i <= weight; i++) {
                if (i == 0) {
                    // First integer group: no leading zeros
                    sb.append(i < ndigits ? digits[i] : 0);
                } else {
                    sb.append(i < ndigits ? String.format("%04d", digits[i]) : "0000");
                }
            }
            // Fractional groups start at index weight+1
            if (weight + 1 < ndigits) {
                sb.append('.');
                for (int i = weight + 1; i < ndigits; i++) {
                    sb.append(String.format("%04d", digits[i]));
                }
            }
        }

        try {
            java.math.BigDecimal result = new java.math.BigDecimal(sb.toString());
            return result.setScale(dscale, java.math.RoundingMode.DOWN);
        } catch (ArithmeticException | NumberFormatException e) {
            throw new Codec.ParseException("Invalid binary numeric: " + e.getMessage());
        }
    }

    private static boolean isNumericChar(char c) {
        return (c >= '0' && c <= '9') || c == '.' || c == '+' || c == '-' || c == 'e' || c == 'E';
    }

}
