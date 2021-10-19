#!/usr/bin/bash
LOGDIR=./LOGS
mkdir -p -m770 $LOGDIR
scancel -n'IBCSO_plot_SRTM'
scancel -n'IBCSO_plot_IBCSO1'

rm IBCSO2_*_global.png 2>/dev/null

sbatch -J'IBCSO_plot_SRTM'  --mem=64G --qos=short --partition=mini,fat,xfat --time=0:29:00 --mail-user=sviquera@awi.de --output=${LOGDIR}/03_plot_SRTM.log --error=${LOGDIR}/03_plot_SRTM.err ./SRUN/03_IBCSO_plot.srun 0
sbatch -J'IBCSO_plot_IBCSO1'  --mem=64G --qos=short --partition=mini,fat,xfat --time=0:29:00 --mail-user=sviquera@awi.de --output=${LOGDIR}/03_plot_IBCSO.log --error=${LOGDIR}/03_plot_IBCSO.err ./SRUN/03_IBCSO_plot.srun 1
