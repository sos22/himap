#! /bin/bash

set -e

if ! [ -d "import" ]
then
    mkdir import
    find "$1" -type f > import/todo
    echo 1 > import/cursor
    touch import/failed
fi

read cursor < import/cursor

tail -n +${cursor} import/todo | while read fname
do
    echo "Import $fname"
    if ! ./dist/build/harbinger-mda/harbinger-mda < $fname
    then
	echo $fname >> import/failed
    fi
    cursor=$(($cursor + 1))
    echo $cursor > import/cursor
done