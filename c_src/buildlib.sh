#!/bin/sh

WORKDIR=`pwd`/system
TARGETDIR=`(cd .. && pwd)`/priv

DB_VER="4.7.25"

## Check for necessary tarball
if [ ! -f "db-${DB_VER}.tar.gz" ]; then
    echo "Could not find db tarball. Aborting..."
    exit 1
fi

## Make sure target directory exists
mkdir -p $TARGETDIR

## Remove existing directories
rm -rf system db-${DB_VER}

## Untar and build everything
tar -xzf db-${DB_VER}.tar.gz && \
(cd db-${DB_VER}/build_unix && \
    ../dist/configure --prefix=$WORKDIR --disable-shared --with-pic && make && ranlib libdb-*.a && make install) && \
    mkdir -p $TARGETDIR/utils && \
    rm -rf db-${DB_VER}




