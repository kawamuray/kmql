#!/bin/bash
exec java -cp "$0:$CLASSPATH" kmql.Kmql "$@"
