#!/bin/bash

BASEDIR=`dirname $0`
INPUTDIR=$BASEDIR/input
OUTPUTDIR=$BASEDIR/output
PRG=$BASEDIR/../parqueteur/parqueteur

# run_test <file name from input dir> <parqueteur command> <command options>
function run_test {
    local file=$1
    local cmd=$2
    # TODO: support multiple options
    local options=$3

    echo $cmd $options $file

    local input=$INPUTDIR/$file.parquet
    if [ ! -f $input ] ; then
        echo "ERROR: input file $input doesn't exist"
        return 1
    fi



    # TODO: remove some characters like command or space from options
    local expected_out=$OUTPUTDIR/$cmd/$file$options.out
    if [ ! -f $expected_out ] ; then
        echo "ERROR: file $expected_out with the expected output doesn't exist"
        return 2
    fi

    local actual_out=`mktemp`.out

    $PRG $cmd $options $input 1>$actual_out
    local e=$?
    if [ $e -ne 0 ] ; then
        echo "FAIL: non-zero return code $e"
        return 3
    fi

    diff -q -u $actual_out $expected_out
    e=$?
    if [ $e -ne 0 ] ; then
        echo "FAIL"
        return $e
    else
        echo "PASS"
        return 0
    fi
}

run_test Min1 meta -json
run_test Min1 schema
