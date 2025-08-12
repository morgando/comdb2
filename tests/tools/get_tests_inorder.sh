#!/usr/bin/env bash
# Get list of tests in order from longest to shortest duration

SKIPLONGRUNNING=0
SHOWTIMEOUT=
LONGESTFIRST=1

extension="$1"

while true ; do
  case "$2" in
  --skip-long-running) SKIPLONGRUNNING=1; shift;;
  --show-timeout) SHOWTIMEOUT=1; shift;;
  --shortest-first) LONGESTFIRST=0; shift;;
  * ) break;;
  esac
done

DEFAULT_TIMEOUT=5  # default timeout for the tests is 5 seconds

#get the tests with custom times
custom_times=`grep -H "TEST_TIMEOUT=" *.${extension}/Makefile | sed 's#export TEST_TIMEOUT=##; s#/Makefile:# #; s#m$##'`

#get the tests with default times
default_times=`grep -Hc "TEST_TIMEOUT=" *.${extension}/Makefile  | grep ":0" | sed "s#/Makefile:0# $DEFAULT_TIMEOUT#"`
IFS=$'\n'; 

#get the generated tests

generated_test_pattern="*.${extension}/*.testopts"
# if no files match generated_test_pattern, then we don't want
# the glob pattern to be returned (which is what will happen if we don't unset nullglob)
shopt -s nullglob

generated=$(for j in ${generated_test_pattern}; do 
    basedir=${j%%/*}
    time=`echo -e "${custom_times} \n${default_times}" | grep "^$basedir" | awk '{print $2}'`
    echo $j $time
done | sed "s#.${extension}/#_#g; s#\.testopts#_generated.${extension}#g;" )
shopt -u nullglob

F=${SHOWTIMEOUT:+,\$2}
if [ "$LONGESTFIRST" -eq 1 ] ; then
    SORTORDER="r"
fi

echo -e "${custom_times} \n${default_times} \n${generated}" | sort -k2 -t' ' -n${SORTORDER} | awk '{ if(!'$SKIPLONGRUNNING' || $2 < 120) { print $1'$F' } }'
