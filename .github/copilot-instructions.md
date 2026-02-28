# Summary

This library ports the Haskell postgresql-types library to Java. It provides optimal lossless representations of PostgreSQL types, codecs for encoding and decoding PostgreSQL types in both text and binary formats accompanied by metadata such as type OIDs and names. The library also includes integration tests to ensure the correctness of the codecs.

For every PostgreSQL type, the library defines a dedicated Java class that provides basic conversion functionality and codec definition.

The library does not cover the type constraints of the PostgreSQL types, such as the length of a varchar or the precision of a numeric.

# References

Use the Haskell library at master (https://github.com/nikita-volkov/postgresql-types) as a reference for tested implementations of standard codecs.

Study its past version (https://github.com/nikita-volkov/postgresql-types/tree/ebadd76c7bc55a3dcc777c89cc404f8ca3c5dbf3) to learn the sketches of a technique of assembling the scalar encoders into composites and arrays. Also study the https://github.com/nikita-volkov/hasql codebase to see a similar but less well abstracted technique, which however is production-tested.

Study https://www.npgsql.org/doc/dev/type-representations.html for a sort of a spec on the binary data format.

# Testing

Use quickcheck (https://github.com/pholser/junit-quickcheck) and testcontainers to test codecs against a real PostgreSQL instance and simulate various edge cases.
