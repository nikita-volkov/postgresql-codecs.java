package io.pgenie.postgresqlCodecs.types;

import java.util.Random;

/**
 * PostgreSQL {@code macaddr8} type. 8-byte MAC (Media Access Control) address in EUI-64 format.
 *
 * <p>Represents an 8-byte MAC address stored as eight individual bytes.
 * The canonical text format is {@code xx:xx:xx:xx:xx:xx:xx:xx} in lower-case hexadecimal.
 *
 * <p>Port of the Haskell {@code PostgresqlTypes.Macaddr8} type.
 */
public record Macaddr8(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {

    /** Generates a random {@code Macaddr8} covering all 8-byte combinations. */
    public static Macaddr8 generate(Random r) {
        byte[] b = new byte[8];
        r.nextBytes(b);
        return new Macaddr8(b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]);
    }

    @Override
    public String toString() {
        return String.format("%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x",
                b1 & 0xff, b2 & 0xff, b3 & 0xff, b4 & 0xff,
                b5 & 0xff, b6 & 0xff, b7 & 0xff, b8 & 0xff);
    }

}
