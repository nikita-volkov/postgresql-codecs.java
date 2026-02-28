package io.pgenie.postgresqlcodecs.types;

import java.util.Arrays;
import java.util.Random;

/**
 * PostgreSQL {@code varbit(n)} type. Variable-length bit string.
 *
 * <p>The bit data is packed into bytes, most-significant bit first. The
 * {@code numBits} field records the actual number of valid bits; any trailing
 * padding bits in the last byte are ignored for equality and hashing.
 *
 * <p>Port of the Haskell {@code PostgresqlTypes.Varbit} type.
 */
public final class Varbit {

    /** Actual number of bits. */
    public final int numBits;
    /** Bit data packed into bytes (MSB first); length == (numBits + 7) / 8. */
    public final byte[] bytes;

    public Varbit(int numBits, byte[] bytes) {
        if (bytes.length != (numBits + 7) / 8) {
            throw new IllegalArgumentException(
                    "bytes.length must be (numBits + 7) / 8, got " + bytes.length
                    + " for numBits=" + numBits);
        }
        this.numBits = numBits;
        this.bytes = bytes;
    }

    /**
     * Creates a {@code Varbit} from a string of {@code '0'} and {@code '1'} characters.
     */
    public static Varbit fromBitString(String s) {
        int n = s.length();
        int nb = (n + 7) / 8;
        byte[] data = new byte[nb];
        for (int i = 0; i < nb; i++) {
            int b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int pos = i * 8 + bit;
                if (pos < n && s.charAt(pos) == '1') b |= (0x80 >>> bit);
            }
            data[i] = (byte) b;
        }
        return new Varbit(n, data);
    }

    /**
     * Generates a random variable-length {@code Varbit} value with 0–64 bits.
     *
     * <p>The number of bits is chosen uniformly from [0, 64]; the bit content is
     * uniformly random, covering the full bit-pattern space for each length.
     */
    public static Varbit generate(Random r) {
        int n = r.nextInt(0, 65);
        if (n == 0) return new Varbit(0, new byte[0]);
        int nb = (n + 7) / 8;
        byte[] data = new byte[nb];
        r.nextBytes(data);
        // Zero out the padding bits in the last byte so that equals() is stable.
        if (n % 8 != 0) {
            data[nb - 1] &= (byte) (0xFF << (8 - (n % 8)));
        }
        return new Varbit(n, data);
    }

    /** Returns the bit string as a string of {@code '0'} and {@code '1'} characters. */
    public String toBitString() {
        StringBuilder sb = new StringBuilder(numBits);
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xff;
            for (int bit = 0; bit < 8; bit++) {
                if (sb.length() < numBits) sb.append((b & (0x80 >>> bit)) != 0 ? '1' : '0');
            }
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Varbit that)) return false;
        return numBits == that.numBits && Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return 31 * numBits + Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "Varbit[numBits=" + numBits + ", bits=" + toBitString() + "]";
    }

}
