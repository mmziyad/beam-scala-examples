# beam-scala-examples

[Apache Beam](https://beam.apache.org) provides Java and Python SDKs for writing the pipelines. 
For writing beam pipelines in scala, there are two options available at this time.

1. Use [Scio](https://github.com/spotify/scio), a scala API for Beam, close to that of Spark and Scalding core APIs.
This API is still not part of Apache Beam, and often lags behind the official Beam releases.

2. Use the [Beam Java SDK](https://beam.apache.org/documentation/sdks/java/) in scala applications.
This repo shows you how to use the Java API in your scala code.

## Wordcount Examples
The examples shown in this repo are same as that of the Java wordcount examples in 
[Beam Example Pipelines](https://github.com/apache/beam/tree/master/examples/java). 
In order to understand the concepts, first go through the code & documentation 
[here](https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples)
and then refer to the scala code for the same examples in this repo.

## License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
