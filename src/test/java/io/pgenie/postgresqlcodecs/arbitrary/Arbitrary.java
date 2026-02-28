package io.pgenie.postgresqlcodecs.arbitrary;

import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import io.pgenie.postgresqlcodecs.types.Inet;
import io.pgenie.postgresqlcodecs.types.Macaddr;

public interface Arbitrary<A> {

    A generate(Random randomizer);

    static <A> Stream<Arguments> samples(Arbitrary<A> arbitrary) {
        var r = new Random();
        return Stream.generate(() -> Arguments.of(arbitrary.generate(r))).limit(100);
    }

    Arbitrary<Inet> INET = Inet::generate;

    Arbitrary<Macaddr> MACADDR = Macaddr::generate;

}
