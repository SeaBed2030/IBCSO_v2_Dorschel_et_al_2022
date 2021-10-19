rm(list=ls())
PROJECTDIR<-'/isibhv/projects/seabed2030/IBCSO2_comp'
setwd(PROJECTDIR)
source(file.path(PROJECTDIR,'IBCSO_comparison_settings_hpc.R'))

#IBCSO2_50 v SRTM
r_ibcso2<-raster(files$IBCSO2_50)
r_srtm<-raster(files$SRTM)

print("raster loaded")

v_ibcso2<-values(r_ibcso2)
v_srtm<-values(r_srtm)

df<-data.frame(x=v_ibcso2,y=v_srtm)

rm(r_ibcso2)
rm(v_ibcso2)
rm(r_srtm)
rm(v_srtm)

df<-df[complete.cases(df),]

write.table(df,file.path(PROJECTDIR,'DATA','COMP_SRTM_data.csv'), sep ='\t',dec='.',col.names=F,row.names=F)