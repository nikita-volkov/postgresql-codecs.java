package io.pgenie.postgresqlCodecs.codecs;

/**
 * PostgreSQL {@code inet} type. IPv4 or IPv6 host address with optional subnet mask.
 *
 * <p>Holds an IPv4 or IPv6 host address, and optionally its subnet, all in one field.
 * The subnet is represented by the number of network address bits present in the host
 * address (the "netmask"). If the netmask is 32 and the address is IPv4 (or 128 for IPv6),
 * the value represents just a single host.
 *
 * <p>Port of the Haskell {@code PostgresqlTypes.Inet} sum type.
 */
public sealed interface Inet permits Inet.V4, Inet.V6 {

    /**
     * IPv4 host address with optional subnet mask.
     *
     * @param address  IPv4 address as a 32-bit big-endian word.
     * @param netmask  Network mask length in the range 0–32.
     */
    record V4(int address, byte netmask) implements Inet {}

    /**
     * IPv6 host address with optional subnet mask.
     *
     * @param w1       First 32 bits of the IPv6 address in big-endian order.
     * @param w2       Second 32 bits of the IPv6 address in big-endian order.
     * @param w3       Third 32 bits of the IPv6 address in big-endian order.
     * @param w4       Fourth 32 bits of the IPv6 address in big-endian order.
     * @param netmask  Network mask length in the range 0–128.
     */
    record V6(int w1, int w2, int w3, int w4, byte netmask) implements Inet {}

}
