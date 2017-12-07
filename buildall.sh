#!/bin/bash

: ${ACTION:=install}

#for M in activemq-rpc; do
#	pushd $M
#	mvn $ACTION
#	popd
#done

for G in graph vertx activemq-rpc utils-zero zkBasedInit coordworkers; do
	pushd $G
	gradle $ACTION
	popd
done

