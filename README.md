This branch is based on a copy of
https://github.com/ericgribkoff/grpc-java/tree/6e2acb1962b821a7226fa27e2bdeb7fdcd385b82/xds/example-xds

How to build:
=============

```sh
git clone https://github.com/grpc/grpc-java.git
pushd grpc-java

# Build grpc-library from master branch and publish to maven local.
./gradlew publishToMavenLocal -x test -PskipCodegen=true -PskipAndroid=true

# Build example application.
git checkout xds-staging
pushd xds/example-xds
../../gradlew installDist
```
