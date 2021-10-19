#!/usr/bin/bash
LOGDIR=./LOGS
mkdir -p -m770 $LOGDIR

scancel -n'IBCSO_merge_IBCSO1'
sbatch -J'IBCSO_merge_IBCSO1'  --mem=64G --partition=mini,fat,xfat --time=12:00:00 --mail-user=sviquera@awi.de --output=${LOGDIR}/01_merge_IBCSO.log --error=${LOGDIR}/01_merge_IBCSO.err ./_SRUN/01_IBCSO_merge.srun 1

scancel -n'IBCSO_merge_SRTM'
sbatch -J'IBCSO_merge_SRTM'  --mem=64G --partition=mini,fat,xfat --time=12:00:00 --mail-user=sviquera@awi.de --output=${LOGDIR}/01_merge_SRTM.log --error=${LOGDIR}/01_merge_SRTM.err ./_SRUN/01_IBCSO_merge.srun 0