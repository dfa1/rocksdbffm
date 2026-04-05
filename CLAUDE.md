Project
----

This is an experimental wrapper for rocksdb using FFM and JDK 25.
Why we are building this? Because there is always a lag between cool stuff happening
in rocksdb C++ and the JNI wrappers. Lately the lag is getting big...

How to
-----

Using the local installation of rocksdb under

/opt/homebrew/Cellar/rocksdb/10.10.1/

use jextract to get all the C endpoints under include/rocksdb/c.h


How to validate
---

For every feature, build it with rocksdbjni and then rebuild it with FFM.
Write unit tests in JUnit 6 and @TemporaryFolder.

What feature to cover
----

- create/open a rocksdb database
- put/get/delete
- batch
- transaction
- and the iterator
- try to cover a minimal amount of Options

