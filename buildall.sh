#!/bin/bash

: ${ACTION:=install}

for M in activemq-rpc; do
	pushd $M
	mvn $ACTION
	popd
done

for G in coordworkers zkBasedInit; do
	pushd $G
	gradle $ACTION
	popd
done

