#!/bin/bash
errLog='LODAnalysis/runAnalysis.err'
log='LODAnalysis/runAnalysis.log'
rm -f $errLog;
rm -f $log;
# Usage info
show_help() {
cat << EOF
Usage: ${0##*/} [-hv] [-f FILE] [-p ROOT_PATH] [ANALYSIS_SCRIPTS]...
Run analysis pipeline on hadoop. When no analysis script(s) is passed via the arguments, we'll run all the scripts at our disposal!

    -h          display this help and exit
    -p PATH     read analysis files from hadoop path (defaults to home dir)
    -o PATH     The hadoop output directory
    -d DIRNAME  only analyze this dataset dir (no need for -p option here)
    -v          verbose mode. Can be used multiple times for increased
                verbosity.
EOF
}
function hadoopDatasetLs {
	hadoopDatasetListing=()
	#we want to get all available datasets in a certain folder. The names are md5 hashed, meaning they should have length 32
	cmd="hadoop fs -ls $1 | grep -e '[a-z0-9]\{32\}$'"
	#echo $cmd;
	#echo "hadoop fs -ls $1";
	if [ "$verbose" -eq 1 ]; then echo "fetching hadoop files: $cmd"; fi
	dirListing=`eval $cmd`
	for word in ${dirListing} ; do
 		if [[ $word =~ ^/ && -n $word ]];then
 			#echo $word;
	    	hadoopDatasetListing+=($word)
	    	#echo ${hadoopDatasetListing[@]}
	    fi
	done
}

#do sanity check (we need to be in the proper directory to make sure the relative paths in for instance our pig scripts can be reached properly)
#just a naive check (check if there is an LODAnalysis dir in the current working dir)
if [ ! -d LODAnalysis ]; then
    echo "Wrong working directory. Cannot locate LODAnalysis path in current working directory.";
	fileDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
	if [[ "$fileDir" == `pwd` ]]; then
		echo "You are currently working in the LODAnalysis directory. You should probably move one dir upwards"
	fi
    exit 1
fi

# Initialize our own variables:
outputPath=""
rootPath="."
datasetDirs=()
verbose=0
analysisScripts=( 'pig LODAnalysis/pig/calcStats.py' 'LODAnalysis/mapReduce/runHadoopJobs.sh GetSchemaStats')
#analysisScripts=( 'LODAnalysis/mapReduce/runHadoopJobs.sh GetSchemaStats')
OPTIND=1 # Reset is necessary if getopts was used previously in the script.  It is a good idea to make this local in a function.
while getopts "hvp:o:d:" opt; do
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
        d)  declare -a datasetDirs=( $OPTARG )
		    ;;
        '?')
            show_help >&2
            exit 1
            ;;
    esac
done
shift "$((OPTIND-1))" # Shift off the options and optional --.


#echo $datasetDirs
#exit;

if [ ${#datasetDirs[@]} -eq 0 ]; then
	echo "fetching ntriple directories from hadoop path '$rootPath'"
	hadoopDatasetLs
	#echo ${hadoopDatasetListing[@]}
	#exit;
	datasetDirs=("${hadoopDatasetListing[@]}")
	if [ ${#datasetDirs[@]} -eq 0 ]; then
		echo "Could not find ntriple directories on hdfs. Root path: $rootPath";
		show_help >&2
		exit 1
	fi
fi

if [ "$#" -eq 0 ]; then
	echo "running all analysis methods at our disposal!"
else
	echo "running the following analysis methods:"
	printf '%s\n' "$@"
	analysisScripts=( "$@" )
fi
#echo ${datasetDirs[@]}
#exit;
numDatasets=${#datasetDirs[@]}
numAnalysis=${#analysisScripts[@]}
datasetCount=1
for datasetDir in "${datasetDirs[@]}"; do
	datasetBasename=`basename $datasetDir`
	analysisCount=1
	for analysisFunction in "${analysisScripts[@]}"; do
		printf "running for: $datasetBasename. Dataset $datasetCount / $numDatasets, Analysis $analysisCount / $numAnalysis \r"
		#echo "$analysisFunction $datasetDir $outputPath"
		#echo $datasetDir;
		cmd="$analysisFunction $datasetDir $outputPath"
		#if [ "$verbose" -eq 1 ]; then echo "running analysis: $cmd"; fi
		echo "running $cmd" >> $log;
		`$cmd >> $log 2>> $errLog`;
		if [[ $? != 0 ]]; then
			echo "Running cmd '$cmd' failed"
		fi;
		analysisCount=`expr $analysisCount + 1`
	done
  #pig LODAnalysis/pig/extractNs.py $inputFile
  datasetCount=`expr $datasetCount + 1`
done
printf "\ndone!\n"
