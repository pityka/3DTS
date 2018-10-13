#!/usr/bin/env bash

# this is for code ocean. assume codeocean.setup.sh is done

target/universal/stage/bin/saturation -Dconfig.file=input/conf -J-Xmx115G -Djava.io.tmpdir=tmp/ -Dfile.encoding=UTF-8