#!/bin/bash
echo "Stopping and removing any existing running containers"

NAME=$1
ACTION=$2
VERSION=$3
PLATFORM=$4
    
REPO="containers.intersystems.com/iscinternal/sds-service"

echo "name = ${NAME}"
echo "action = ${ACTION}"
echo "version = ${VERSION}"
echo "platform = ${PLATFORM}"


IMAGE=${NAME}:${VERSION}-${PLATFORM}
echo "image = ${IMAGE}"

if [ ${ACTION} = "build" ]
then

    if [ ${PLATFORM} = "arm64" ]
    then
        # Build for arm64
        rm -f ./bin/*
        cp ./bin_arm64/* ./bin/
    elif [ ${PLATFORM} = "amd64" ]
    then
        rm -f ./bin/*
        cp ./bin_amd64/* ./bin/
    fi

    echo "building for $platform"
    #docker build --build-arg "ARCH=${PLATFORM}/" -t ${IMAGE} .
    docker buildx build --platform "linux/${PLATFORM}" --output type=docker --build-arg "ARCH=${PLATFORM}/" -t ${IMAGE} .
    docker tag ${IMAGE} ${REPO}/${IMAGE}
    #docker buildx build --platform ${docker_platform} --output type=docker --tag stratify:${VERSION} .
fi

if [ ${ACTION} = "push" ]
then
    docker push ${REPO}/${IMAGE}
fi

if [ ${ACTION} = "manifest" ]
then
    # Creating without the platform -
    # TODO the platforms are fixed but should be parameterized as -arm64 and -amd64 extensions are magic strings
    docker manifest create ${REPO}/${NAME}:${VERSION} \
    --amend ${REPO}/${NAME}:${VERSION}-amd64 \
    --amend ${REPO}/${NAME}:${VERSION}-arm64

    docker manifest push ${REPO}/${NAME}:${VERSION}
fi

if [ ${ACTION} = "run" ]
then
    docker stop ${NAME}
    docker rm ${NAME}

    ## --shm-size=3.78gb added because of warning thrown by Ray
    ## ports: 8153 is for ray server, 8265 for dashboard
    #Volume mount is for local dedbugging purposes only, should not affect anything in deployment but not necessary
    docker run -v $(pwd):/home/ -p 8153:8000 -p 8265:8265 -p 8111:8111 --name ${NAME} -it --shm-size=2gb ${IMAGE} 
    #docker exec -it athena /bin/bash
fi
