Usage
-----

See docs in example project in [../table-walkthrough/](../table-walkthrough/).

Building Docker...
--------

Builds are done inside docker, so minimal setup is needed.

First make scala sources accessible to `Dockerfile`:
```shell
# cp -R ../../../dam-client-scala/  dam-client-scala
```

Then run [./build.sh](./build.sh) to build docker images


Random notes
------


Limitations/TODOs:
- [ ] fully automatic stats extraction too 
  * P.S. ASM5/ASM7 or alike Java bytecode modification would for sure overcome the `FlinkHook` limitations 
    mentioned above (by automatically insert `kensuMarkOutput`), with slight risk of
    bytecode incompatibility issues on runtime
  * automatically inserting `kensuMarkStatsInput` is more complicated as:
      - ~~we do not know which column/field to be used for window `timestamp`~~ (TODO: clarify how to handle DataStats timestamps)
      - possibly want to push-down the filters and projections, so stats take account of these be investigated, but might be sightly limited by above mentioned issues.
  

Supported Flink cluster modes:
  * modified the sample to start Flink in proper cluster mode (seemed necessary for stats computation to work well) - based on https://github.com/apache/flink-playgrounds/tree/master/operations-playground  .
  * p.s. `standalone-job` mode didn't seem to work with FlinkHooks (for automatic tracking)
  * `/opt/flink/bin/flink run --target local your-jar.jar` mode might work too, but not sure about streaming stats



Local dev with IntelliJ
------

```bash
(cd dam-client-scala && SCALA_VERSION="2.12.7" AKKA_VERSION="2.5.21" sbt compile publishM2 )
```
then load this maven project
