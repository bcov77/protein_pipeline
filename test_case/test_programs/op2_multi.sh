#!/bin/bash


output=""

first=""

for var in "$@"
do
    if [ -z $first ]; then
        first="a"
        continue
    fi

    a=$(printf $var; printf "_")
    touch $a
    output=$(echo "$output"; printf "$a"; printf "\n" )



done

echo "$output" > $1

