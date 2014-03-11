#!/bin/sh
# -------------------------------------
# Hamid Bazoobandi
# Vrije Universitiet Amsterdam
# March 2014
# -------------------------------------
workingDir=`basename $PWD`;
mapperTasks=1;
reducerTasks=1
# -------------------------------------
# Checking optional arguments
# -------------------------------------
usage() {
  echo "Usage: $0 <hadoop job name> <input> <output>";
  exit 1;
}
# -------------------------------------
help(){
  echo "Usage: $0 <hadoop job name> <input> <output> [options]"
  echo
  echo "Options:"
  echo "--help              Print this message"
  echo "--reducertasks      Number of reducer tasks (Default value is 1)."
  echo "--mappertasks       Number of mapper tasks (Default would be choosen by hadoop)"
  exit 1;
}
# -------------------------------------
givenMandatoryArgs=0;
for opt do
  optarg=`expr "x$opt" : 'x[^=]*=\(.*\)'`
  case $opt in
    # --------------------------
    --help|-h)
    help;
    ;;
    # --------------------------
    --reducertasks=*)
    if test "$optarg" = ""; then
      echo "Please specify the number of reducer tasks!";
      exit 1;
    fi
    reducerTasks="$optarg"
    ;;
    # --------------------------
    --mappertasks=*)
    if test "$optarg" = ""; then
      echo "Please specify the number of mapper tasks!";
      exit 1;
    fi
    mapperTasks="$optarg"
    ;;
    # --------------------------
    *)
    if test "$opt" == ""; then
      usage;
    fi
    case $givenMandatoryArgs in
      0)
      jobName=$opt;
      ;;
      1)
      inputDir=$opt;
      ;;
      2)
      outputDir=$opt;
      ;;
      *)
      ;;
    esac
    givenMandatoryArgs=`expr 1 + $givenMandatoryArgs`;
    ;;
  esac
done

# -------------------------------------
# Checking for mandatory arguments
# -------------------------------------
if [[ $givenMandatoryArgs -lt 3 ]]; then
  usage;
fi
# -------------------------------------
if [[  $workingDir == "LODAnalysis" ]]; then
  path="mapReduce";
elif [[ $workingDir == "mapReduce" ]]; then
  path=".";
else
  echo "You are in the wrong working dir";
  exit;
fi

hadoop jar $path/lib/datasetAnalysisTools.jar jobs.$jobName $inputDir $outputDir --reducetasks $reducerTasks
