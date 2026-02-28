package io.pgenie.postgresqlcodecs.types;

import java.util.Random;

/**
 * PostgreSQL {@code cidr} type. IPv4 or IPv6 network specification.
 *
 * <p>Holds an IPv4 or IPv6 network specification (network address + netmask) where host
 * bits must be zero. If you want to store individual host addresses with optional netmasks,
 * use {@link Inet} instead.
 *
 * <p>Port of the Haskell {@code PostgresqlTypes.Cidr} sum type.
 */
public sealed interface Cidr permits Cidr.V4, Cidr.V6 {

    /**
     * IPv4 network with subnet mask.
     *
     * @param address  IPv4 network address as a 32-bit big-endian word (host bits must be zero).
     * @param netmask  Network mask length in the range 0–32.
     */
    record V4(int address, byte netmask) implements Cidr {}

    /**
     * IPv6 network with subnet mask.
     *
     * @param w1       First 32 bits of the IPv6 network address in big-endian order.
     * @param w2       Second 32 bits of the IPv6 network address in big-endian order.
     * @param w3       Third 32 bits of the IPv6 network address in big-endian order.
     * @param w4       Fourth 32 bits of the IPv6 network address in big-endian order.
     * @param netmask  Network mask length in the range 0–128.
     */
    record V6(int w1, int w2, int w3, int w4, byte netmask) implements Cidr {}

    /**
     * Generates a random {@code Cidr} value with host bits correctly zeroed to match
     * the netmask, covering both IPv4 and IPv6 networks across the full mask range.
     */
    static Cidr generate(Random r) {
        if (r.nextBoolean()) {
            // IPv4: mask 0–32, host bits zeroed
            int mask = r.nextInt(0, 33);
            int addr = mask == 0 ? 0 : r.nextInt() & (int) (0xFFFFFFFFL << (32 - mask));
            return new V4(addr, (byte) mask);
        } else {
            // IPv6: mask 0–128, host bits zeroed
            int mask = r.nextInt(0, 129);
            int w1 = mask >= 32 ? r.nextInt() : (mask == 0 ? 0 : r.nextInt() & (int) (0xFFFFFFFFL << (32 - mask)));
            int w2 = mask >= 64 ? r.nextInt() : (mask <= 32 ? 0 : r.nextInt() & (int) (0xFFFFFFFFL << (64 - mask)));
            int w3 = mask >= 96 ? r.nextInt() : (mask <= 64 ? 0 : r.nextInt() & (int) (0xFFFFFFFFL << (96 - mask)));
            int w4 = mask >= 128 ? r.nextInt() : (mask <= 96 ? 0 : r.nextInt() & (int) (0xFFFFFFFFL << (128 - mask)));
            return new V6(w1, w2, w3, w4, (byte) mask);
        }
    }

}
