#!/bin/sh

PREFIX=/is/app/o3
PYTHONCF=$PREFIX/bin/python-config
export LD_LIBRARY_PATH=$PREFIX/lib
FLAGS="$($PYTHONCF --cflags) -L$PREFIX/lib -lpthread -ldl -lutil -lm -lpython2.5"

echo gcc -o $PREFIX/base/beams beams.c -D__BOOTNAME__='"bootup"' $FLAGS
gcc -o $PREFIX/base/beams beams.c -D__BOOTNAME__='"bootup"' $FLAGS
gcc -o $PREFIX/base/beam beams.c -D__BOOTNAME__='"autobootup"' $FLAGS
