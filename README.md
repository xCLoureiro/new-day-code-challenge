# new-day-code-challenge

This project was developed and tested using the following versions:

* OpenJDK 64-Bit v1.8.0_131
* Scala v2.10.6
* SBT v0.13.15
* Apache Spark v1.6.2 

## Testing

```sh
sbt test
```

You can also check the coverage of the tests:
```sh
sbt clean coverage test coverageReport
```

## Usage

Generate the "Fat" .jar file with the solution:
```sh
sbt assembly
```

Run the solution:
```sh
./run_spark.sh <jar_file> <moviesFile> <ratingsFile> <outDir (without last /)>
```
