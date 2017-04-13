#!/bin/bash

/usr/src/kernels/3.10.0-514.10.2.el7.x86_64/scripts/sign-file sha256 \
    ./signing_key.priv ./signing_key.x509 ../sg_agent.ko
