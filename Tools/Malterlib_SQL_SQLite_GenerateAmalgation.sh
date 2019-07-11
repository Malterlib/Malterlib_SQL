#!/bin/bash

set -e

Sources="$1"
OutputDir="$2"
TempDir="$3"
BaseDirectory="$4"
MToolExe="$5"

DependencyFile="$OutputDir/SQLite.MalterlibDependency"

echo DependencyFile="$DependencyFile"

if [ -e "$DependencyFile" ]; then
	cd "$BaseDirectory"
	"$MToolExe" CheckDependencies Relative=true Verbose=true "Directory=$OutputDir"
fi

if [ -e "$OutputDir/sqlite3.c" ] && [ -e "$DependencyFile" ]; then
	echo SQLite is up to date. To force rebuild:
	echo rm -f \"$OutputDir/sqlite3.c\"
	exit 0
fi


echo Sources="$Sources"
echo OutputDir="$OutputDir"
echo TempDir="$TempDir"
echo BaseDirectory="$BaseDirectory"

mkdir -p "$OutputDir"

rm -rf "$TempDir"
mkdir -p "$TempDir"

cd "$TempDir"
"$Sources/configure"
make sqlite3.c

mv "sqlite3.c" "$OutputDir"
mv "sqlite3.h" "$OutputDir"
mv "sqlite3ext.h" "$OutputDir"

ExcludePatterns="*/.git"
ExcludePatterns="$ExcludePatterns;*/.DS_Store"
ExcludePatterns="$ExcludePatterns;*/test"
ExcludePatterns="$ExcludePatterns;*/*.test"

if [[ "$PlatformFamily" != "Windows" ]] ; then
	function ConvertPath()
	{
		echo "$1"
	}
else
	function ConvertPath()
	{
		cygpath -m "$1"
	}
fi

cd "$BaseDirectory"

"$MToolExe" BuildDependencies UseHash=true Relative=true \
	"OutputFile=`ConvertPath \"$DependencyFile\"`" \
	"Output:`ConvertPath \"$OutputDir/sqlite3.c\"`" \
	"Output:`ConvertPath \"$OutputDir/sqlite3.h\"`" \
	"Output:`ConvertPath \"$OutputDir/sqlite3ext.h\"`" \
	"Input:`ConvertPath \"${BASH_SOURCE[0]}\"`" \
	"Find:`ConvertPath \"$Sources\"`/*;RIF;32;$ExcludePatterns"
