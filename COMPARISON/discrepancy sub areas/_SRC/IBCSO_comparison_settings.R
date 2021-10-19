library(raster)
library(sp)
library(rasterVis)
library(ggplot2)
library(pracma)

IBCSO_proj<-sp::CRS("+proj=stere +ellps=WGS84 +lat_0=-90 +lon_0=0dE +lat_ts=-65")
ANT_POL_proj<-sp::CRS("+init=epsg:3031")

colours=list(IBCSO_blue='#2c6996',IBCSO_orange='#FF5601',IBCSO_dark='#3A3A3A')

IBCSO_palette<-colorRampPalette(c(colours$IBCSO_blue, colours$IBCSO_orange))

tiles<-file.path(getwd(),'/SHAPES/IBCSOv2_tiles.gpkg')
IBCSO1<-file.path(getwd(),'/SHAPES/IBCSO_v1_bed_only_ocean.tif')
IBCSO2<-file.path(getwd(),'/SHAPES/IBCSO_v2_bed_only_ocean.tif') #C:\Users\bax3447\Desktop\LATEST PRODUCT\IBCSO_current_version\RASTER
SRTM<-file.path(getwd(),'/SHAPES/SRTM15plus_V2-2_only_ocean.tif')
OCEAN<-file.path(getwd(),'SHAPES','ocean_mask.tif')
AOI_TILES<-file.path(getwd(),'/SHAPES/IBCSOv2_technical-validation_areas_selection.gpkg')

RESDIR<-file.path(getwd(),'RESULTS')
dir.create(RESDIR,recursive=T,showWarnings = F)

checkDistribution<-function(x,PLOT_STATS=F,xrange=range(x),...){
  x<-x[!is.na(x)]
  x<-subset(x, x >= xrange[1] & x <= xrange[2])
  h<-hist(x,axes=F,ann=F)
  
  f<-ecdf(x)
  xE<-seq(min(x),max(x),abs(max(x)-min(x))*1E-3)
  yE<-f(xE)*max(h$counts)
  d<-density(x,na.rm=T)
  yA<-approxfun(x=d$x,y=d$y,yleft=0,yright=0)
  
  grid()
  d$y_<-d$y/max(d$y)*max(h$counts)
  yA<-yA(xE)/max(yA(xE))*max(h$counts)
  
  h<-hist(x,add=T,col=adjustcolor('blue',.4))
  lines(xE,yE,col='orange',lwd=2)
  lines(yA~xE,col=adjustcolor('red'),lwd=2)
  #lines(yNorm~xE,col='green',lwd=2)
  abline(v=median(x),col='purple',lty=2,lwd=2)
  abline(v=0,col='green')
  rug(x,side=3,col='red',ticksize = 0.02)
  axis(1)
  axis(2)
  axis(4,at=seq(0,max(h$counts),max(h$counts)/5),labels=seq(0,1,1/5))
  box()
  title(ylab='counts',...)
  #return(list(SW=sw_test,KS=ks_test))
}