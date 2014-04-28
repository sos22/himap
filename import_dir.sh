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

if [ -s import/failed ]
then
    echo "Some imports failed; list in import/failed"
    exit 1
else
    echo "Import successful"
    rm -r import
    exit 0
fi
