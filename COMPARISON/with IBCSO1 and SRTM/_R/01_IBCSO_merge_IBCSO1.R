rm(list=ls())
PROJECTDIR<-'/isibhv/projects/seabed2030/IBCSO2_comp'
setwd(PROJECTDIR)
source(file.path(PROJECTDIR,'IBCSO_comparison_settings_hpc.R'))

#IBCSO2_60 v IBCSO 1
r_ibcso2<-raster(files$IBCSO2_60)
r_ibcso1<-raster(files$IBCSO1)

print("raster loaded")

v_ibcso2<-values(r_ibcso2)
v_ibcso1<-values(r_ibcso1)

df<-data.frame(x=v_ibcso2,y=v_ibcso1)

rm(r_ibcso2)
rm(v_ibcso2)
rm(r_ibcso1)
rm(v_ibcso1)

df<-df[complete.cases(df),]

write.table(df,file.path(PROJECTDIR,'DATA','COMP_IBCSO1_data.csv'), sep ='\t',dec='.',col.names=F,row.names=F)