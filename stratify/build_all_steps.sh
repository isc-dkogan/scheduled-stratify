#!/bin/bash

NAME=$1
VERSION=$2

./build.sh $NAME build ${VERSION} arm64
./build.sh $NAME build ${VERSION} amd64

./build.sh $NAME push ${VERSION} amd64
./build.sh $NAME push ${VERSION} arm64

./build.sh $NAME manifest ${VERSION} --