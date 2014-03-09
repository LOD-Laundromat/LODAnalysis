#!/bin/bash

# Usage info
show_help() {
cat << EOF
Usage: ${0##*/} [-hv] [-f FILE] [-p ROOT_PATH] [ANALYSIS_SCRIPTS]...
Run analysis pipeline on hadoop. When no analysis script(s) is passed via the arguments, we'll run all the scripts at our disposal!

    -h          display this help and exit
    -p PATH     read analysis files from hadoop path
    -o PATH     The hadoop output directory
    -f FILE     only analyze this hadoop ntriple file (no need for -p option here)
    -v          verbose mode. Can be used multiple times for increased
                verbosity.
EOF
}
function hadoopLs {
	hadoopListing=()
	cmd="hadoop fs -ls $1 | grep -e '\.nt\(\.gz\)*$'"
	#echo "hadoop fs -ls $1";
	if [ "$verbose" -eq 1 ]; then echo "fetching hadoop files: $cmd"; fi
	dirListing=`eval $cmd`
	for word in ${dirListing} ; do
 		if [[ $word =~ ^/ ]];then
	    	hadoopListing+=(${word})
	    fi
	done
}

#do sanity check (we need to be in the proper directory to make sure the relative paths in for instance our pig scripts can be reached properly)
#just a naive check (check if there is an LODAnalysis dir in the current working dir)
#if [ ! -d LODAnalysis ]; then
#    echo "Wrong working directory. Cannot locate LODAnalysis path in current working directory";
#    exit 1
#fi

# Initialize our own variables:
outputPath=""
rootPath=""
inputFiles=()
verbose=0
analysisScripts=( 'pig LODAnalysis/pig/extractNs.py', 'LODAnalysis/mapReduce/runHadoopJobs.sh GetSchemaStats')
OPTIND=1 # Reset is necessary if getopts was used previously in the script.  It is a good idea to make this local in a function.
while getopts "hvpo:f:" opt; do
    case "$opt" in
        h)
            show_help
            exit 0
            ;;
        v)  verbose=1
            ;;
        p)  rootPath=$OPTARG
            ;;
        o)  outputPath=$OPTARG
            ;;
        f)  inputFiles=( $OPTARG )
		    ;;
        '?')
            show_help >&2
            exit 1
            ;;
    esac
done
shift "$((OPTIND-1))" # Shift off the options and optional --.


if [ "$#" -eq 0 ]; then
	echo "running all analysis methods at our disposal!"
else
	echo "running the following analysis methods:"
	printf '%s\n' "$@"
	analysisScripts=( "$@" )
fi



if [ ${#inputFiles[@]} -eq 0 ]; then
	if [ "$rootPath" ]; then
		echo "fetching ntriple directories from hadoop"
		hadoopLs
		inputFiles=$hadoopListing
		if [ ${#inputFiles[@]} -eq 0 ]; then
			echo "Could not find ntriple directories on hdfs. Root path: $rootPath";
			exit 1
		fi
	else
		echo "No root path -and- no input file defined in settings. Cannot analyze ntriples"
		show_help >&2
		exit 1
	fi
fi


for inputFile in "${inputFiles[@]}"; do
	for analysisFunction in "${analysisScripts[@]}"; do
		`$analysisFunction $inputFile $outputPath`;
	done
  #pig LODAnalysis/pig/extractNs.py $inputFile
done
