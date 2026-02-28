package io.pgenie.postgresqlCodecs.types;

import java.util.Random;

/**
 * PostgreSQL {@code macaddr} type. MAC (Media Access Control) address.
 *
 * <p>Represents a 6-byte MAC address stored as six individual bytes.
 * The canonical text format is {@code xx:xx:xx:xx:xx:xx} in lower-case hexadecimal.
 *
 * <p>Port of the Haskell {@code PostgresqlTypes.Macaddr} type.
 */
public record Macaddr(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6) {

    /** Generates a random {@code Macaddr} covering all 6-byte combinations. */
    public static Macaddr generate(Random r) {
        byte[] b = new byte[6];
        r.nextBytes(b);
        return new Macaddr(b[0], b[1], b[2], b[3], b[4], b[5]);
    }

    @Override
    public String toString() {
        return String.format("%02x:%02x:%02x:%02x:%02x:%02x",
                b1 & 0xff, b2 & 0xff, b3 & 0xff, b4 & 0xff, b5 & 0xff, b6 & 0xff);
    }

}
