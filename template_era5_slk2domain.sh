#!/bin/bash
#
########################################################################################
# Retrieving global ERA5 caf-file formatted data for use with ICON-CLM or COSMO-CLM
# Cutting out a requested region
# The data will be zipped internally. The file size is about 35% of the original then.
#   Do not forget to apply "nccopy -k 2 " to the data before using them with CDO or NCO.
########################################################################################
#
#################################################
# batch settings for preprocessing run on levante at DKRZ
#################################################
#SBATCH --job-name=E5_CR_23
#SBATCH --partition=compute     # Specify partition name for job execution
#SBATCH --ntasks=12             # Specify max. number of tasks to be invoked
#SBATCH --mem=60G
#SBATCH --time=00-08:00:00   # maximum is 8 hours
#SBATCH --output=retera5_%j.log
#SBATCH --error=retera5_%j.log
#SBATCH --account=bb1461    # replace this with your actual project account 
# Set the working directory

set -e

STARTTIME=$(date +%s)
 
module load slk
module load nco

######### User settings start

### if you want to run several months, please check how long one month will take first
START_YEAR=2023
START_MONTH=02
END_YEAR=2023
END_MONTH=04
HINCBOUND=01    # can be 1, 2, 3, 4, or 6

# EURO-CORDEX
#STARTLON=-69.0
#ENDLON=84.0
#STARTLAT=19.0
#ENDLAT=82.0

# Arctic CORDEX
#STARTLON=-180.0
#ENDLON=180.0
#STARTLAT=55.0
#ENDLAT=90.0

# CORDEX Australasia
#STARTLON=80.0
#ENDLON=-140.0
#STARTLAT=-62.0
#ENDLAT=22.0

# Change this for your domain
STARTLON=-100.0
ENDLON=-70.0
STARTLAT=1.0
ENDLAT=20.0

LEVELSTART=40   # count from top

OUTPUT_COMPRESSION=1  # 0 = no compression, 1 = internal netCDF zipping ; compression is lossless
                         # if = 1 , file size is about 35% of the original. Use nccopy -k 2 to unzip it
                         #     before using CDO, NCO commands for analysis or using it in your simulation

WORKDIR=/work/bb1461/era5              # the final output goes here
SCRATCHDIR=/scratch/b/b383260           # will temporarily hold the global data

##################### Generally no changes should be necessary beyond this line ################################

DATE_START=$(date +%s)
#... set maximum number of parallel processes
NTASKS=${SLURM_NTASKS}
#(( MAXPP=NTASKS+NTASKS ))
MAXPP=${NTASKS}
echo maximum number of tasks: ${MAXPP}

YEAR=${START_YEAR}
LAST_YEAR=${END_YEAR}
LAST_MONTH=${END_MONTH}
#... ----- Jahres und Monatsschleife START

while [ ${YEAR} -le ${LAST_YEAR} ]
do

  if [ ${YEAR} -eq ${START_YEAR} ]
  then
    MONTH=${START_MONTH}
  else
    MONTH=01
  fi

  if [ ${YEAR} -eq ${END_YEAR} ]
  then
    LAST_MONTH=${END_MONTH}
  else
    LAST_MONTH=12
  fi

  while [ ${MONTH} -le ${LAST_MONTH} ]
  do

    cd ${SCRATCHDIR}
#... retrieve data from slk archive
    echo retrieve data from slk archive ...
    mkdir -p ${SCRATCHDIR}/retrieve
    lfs setstripe -S 4M -c 8 ${SCRATCHDIR}/retrieve
#    This is what has to be retrieved:
#    slk retrieve /arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}/ERA5_${YEAR}_${MONTH}_part1.tar ${SCRATCHDIR}/retrieve
#    slk retrieve /arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}/ERA5_${YEAR}_${MONTH}_part2.tar ${SCRATCHDIR}/retrieve
#    slk retrieve /arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}/ERA5_${YEAR}_${MONTH}_part3.tar ${SCRATCHDIR}/retrieve
#    slk retrieve /arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}/ERA5_${YEAR}_${MONTH}_part4.tar ${SCRATCHDIR}/retrieve
#    slk retrieve /arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}/ERA5_${YEAR}_${MONTH}_part5.tar ${SCRATCHDIR}/retrieve
#    slk retrieve /arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}/ERA5_${YEAR}_${MONTH}_part6.tar ${SCRATCHDIR}/retrieve
#    slk retrieve /arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}/ERA5_${YEAR}_${MONTH}_part7.tar ${SCRATCHDIR}/retrieve
#    slk retrieve /arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}/ERA5_${YEAR}_${MONTH}_part8.tar ${SCRATCHDIR}/retrieve
#
#    This is the short form in one retrieve command that does not work, because slk retrieve do not accept wild cards:
#    slk retrieve /arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}/ERA5_${YEAR}_${MONTH}_part*.tar ${SCRATCHDIR}/retrieve
#
#    This is the solution provided bei DKRZ
	SEARCH_ID=$(slk_helpers search_limited "{\"\$and\": [{\"path\": {\"\$gte\": \"/arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}\"}}, {\"resources.name\": {\"\$regex\": \"^ERA5_${YEAR}_${MONTH}_part[0-9].tar\"}}]}")
 	SEARCH_ID=$(echo ${SEARCH_ID} | tail -n 1 | sed 's/[^0-9]*//g' )
 	sleep 10
 	slk retrieve ${SEARCH_ID} ${SCRATCHDIR}/retrieve

    mkdir -p ${YEAR}_${MONTH}
    cd ${YEAR}_${MONTH}
    echo ... untar global data

    COUNTPP=0
#    for FILE in $(ls -1 ${SCRATCHDIR}/retrieve/ERA5_${YEAR}_${MONTH}_part?.tar)
    for FILE in $(ls -1 ${SCRATCHDIR}/retrieve/arch/pd1309/forcings/reanalyses/ERA5/year${YEAR}/ERA5_${YEAR}_${MONTH}_part?.tar)
    do
      tar -xf ${FILE} &
      (( COUNTPP=COUNTPP+1 ))
      if [ ${COUNTPP} -ge ${MAXPP} ]
      then
        COUNTPP=0
        wait
      fi
    done
    wait

#    rm ${SCRATCHDIR}/ERA5_${YEAR}_${MONTH}_part?.tar

    cd ${WORKDIR}
    mkdir -p ${YEAR}_${MONTH}

    echo ... cutting out the chosen domain

#... select only files for the time frequency requested
   case "$(printf %02d ${HINCBOUND})" in
        01) echo untar every hour
            FILELIST=$(ls -1 ${SCRATCHDIR}/${YEAR}_${MONTH}/*)
            ;;
        02) echo untar every 02 hours
            FILELIST=$(ls -1 ${SCRATCHDIR}/${YEAR}_${MONTH}/*[012][02468].nc)
            ;;
        03) echo untar every 03 hours
            FILELIST="$(ls ${SCRATCHDIR}/${YEAR}_${MONTH}/*[0][0369].nc) $(ls ${SCRATCHDIR}/${YEAR}_${MONTH}/*[1][258].nc) $(ls -1 ${SCRATCHDIR}/${YEAR}_${MONTH}/*[2][1].nc)"
            ;;
        04)  echo untar every 04 hours
            FILELIST="$(ls ${SCRATCHDIR}/${YEAR}_${MONTH}/*[0][048].nc) $(ls ${SCRATCHDIR}/${YEAR}_${MONTH}/*[1][26].nc) $(ls -1 ${SCRATCHDIR}/${YEAR}_${MONTH}/*[2][0].nc)"
            ;;
        06)  echo untar every 06 hours
             FILELIST="$(ls ${SCRATCHDIR}/${YEAR}_${MONTH}/*[0][06].nc) $(ls ${SCRATCHDIR}/${YEAR}_${MONTH}/*[1][28].nc)"
            ;;
        *) echo ERROR: Invalid HINCBOUND = $(printf %02d ${HINCBOUND})  must be 01, 02, 03, 04 or 06
           exit
            ;;
    esac

    COUNTPP=0
#    FILELIST=$(ls -1 ${SCRATCHDIR}/${YEAR}_${MONTH})
    if [ ${OUTPUT_COMPRESSION} -eq 0 ] 
    then
      NCKS_OPTS="-6"
    else
      NCKS_OPTS="-4 -L 1"
    fi
    for FILE in ${FILELIST}
    do
(
      FILE_COUNTPP=$(basename ${SCRATCHDIR}/${YEAR}_${MONTH}/${FILE})
      CASFILE_COUNTPP=cas${FILE_COUNTPP:3:10}
# no internal zipping
#      ncks -6 -O -F -d level1,${LEVELSTART},138 -d level,${LEVELSTART},137 -d lon,${STARTLON},${ENDLON} -d lat,${STARTLAT},${ENDLAT} ${SCRATCHDIR}/${YEAR}_${MONTH}/${FILE} ${WORKDIR}/${YEAR}_${MONTH}/${CASFILE_COUNTPP}.nc &
# internal lossless zipping, file size is about 35% of the original. Use nccopy -k 2 to unzip it before using CDO, NCO commands.
#      ncks -4 -L 1 -O -F -d level1,${LEVELSTART},138 -d level,${LEVELSTART},137 -d lon,${STARTLON},${ENDLON} -d lat,${STARTLAT},${ENDLAT} ${SCRATCHDIR}/${YEAR}_${MONTH}/${FILE} ${WORKDIR}/${YEAR}_${MONTH}/${CASFILE_COUNTPP}.ncz &
       ncks ${NCKS_OPTS} -O -F -d level1,${LEVELSTART},138 -d level,${LEVELSTART},137 -d lon,${STARTLON},${ENDLON} -d lat,${STARTLAT},${ENDLAT} ${FILE} ${WORKDIR}/${YEAR}_${MONTH}/${CASFILE_COUNTPP}.ncz 
)&
      (( COUNTPP=COUNTPP+1 ))
      if [ ${COUNTPP} -ge ${MAXPP} ]
      then
        COUNTPP=0
        wait
      fi
    done
    wait

# { inserted by b383260: create .tar file for SPICE input
	mkdir -p ${WORKDIR}/year${YEAR}
    cd ${WORKDIR}
    
    tar -cvf year${YEAR}/ERA5_${YEAR}_${MONTH}.tar ${YEAR}_${MONTH}/cas*.ncz
# }

    (( MONTH=10#${MONTH}+1 ))
    MONTH=$(printf %02d ${MONTH})
  done    #... monthly loop END

  let "YEAR = YEAR +1"
done  #... yearly loop END

DATE_END=$(date +%s)
echo time used: $((${DATE_END} - ${DATE_START})) sec


