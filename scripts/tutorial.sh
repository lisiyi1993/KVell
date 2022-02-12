#!/bin/bash
scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
tutorialDir="${scriptDir}/.."
tcmalloc="env LD_PRELOAD=${HOME}/gperftools/.libs/libtcmalloc.so "

rm -f /data/sli144/scratch*/kvell/*

cp ${tutorialDir}/tutorial.c ${tutorialDir}/tutorial.c.bak
cat ${tutorialDir}/tutorial.c | perl -pe 's://.nb_load_injectors = 4:.nb_load_injectors = 4:' | perl -pe 's:[^/].nb_load_injectors = 12: //.nb_load_injectors = 12:' | perl -pe 's:[^/]ycsb_e_uniform,: //ycsb_e_uniform,:' | perl -pe 's://ycsb_a_uniform,:ycsb_a_uniform,:' | perl -pe 's://ycsb_a_zipfian,:ycsb_a_zipfian,:' > ${tutorialDir}/tutorial.c.tmp
mv ${tutorialDir}/tutorial.c.tmp ${tutorialDir}/tutorial.c
make -C ${tutorialDir} -j

echo "Run 1"
${tcmalloc} ${tutorialDir}/tutorial 1 1