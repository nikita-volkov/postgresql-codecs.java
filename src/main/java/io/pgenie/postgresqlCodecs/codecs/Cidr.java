package io.pgenie.postgresqlCodecs.codecs;

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

}
