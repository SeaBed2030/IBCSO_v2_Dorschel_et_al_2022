#!/usr/bin/bash
LOGDIR=./LOGS
mkdir -p -m770 $LOGDIR

rm ./DATA/COMP_*_data_ma_* 2>/dev/null

scancel -n'IBCSO_ma_IBCSO1'
sbatch -J'IBCSO_ma_IBCSO1'  --mem=64G --qos=short --partition=mini,fat,xfat --time=00:29:00 --mail-user=sviquera@awi.de --output=${LOGDIR}/02_ma_IBCSO.log --error=${LOGDIR}/02_ma_IBCSO.err ./SRUN/02_IBCSO_ma.srun 1

scancel -n'IBCSO_ma_SRTM'
sbatch -J'IBCSO_ma_SRTM'  --mem=64G --qos=short --partition=mini,fat,xfat --time=00:29:00 --mail-user=sviquera@awi.de --output=${LOGDIR}/02_ma_SRTM.log --error=${LOGDIR}/02_ma_SRTM.err ./SRUN/02_IBCSO_ma.srun 0