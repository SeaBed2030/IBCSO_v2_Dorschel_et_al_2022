rm(list=ls())
PROJECTDIR<-dirname(rstudioapi::getActiveDocumentContext()$path)
setwd(PROJECTDIR) #only works in RStudio!
SRCDIR<-file.path(PROJECTDIR,'_SRC')
source(file.path(SRCDIR,'IBCSO_comparison_settings.R'))

IBCSO_tiles<-rgdal::readOGR(tiles)
crs(IBCSO_tiles)<-IBCSO_proj

r_IBCSO2<-raster(IBCSO2)
crs(r_IBCSO2)<-IBCSO_proj

AOIS<-rgdal::readOGR(AOI_TILES)
stats<-data.frame()
for (idx in 1:nrow(AOIS@data)){
  AOI<-AOIS$name[idx]
  current_tiles<-AOIS@data$tiles[idx]
  
  if(length(grep('-',current_tiles)) > 0){
    r<-strsplit(current_tiles,'-')[[1]]
    current_tiles<-seq(r[1],r[2])
  }else{current_tiles<-strsplit(current_tiles,';')[[1]]}
  
  comp_raster<-AOIS@data$comparison_grid[idx]
  if (comp_raster == 'IBCSO v1'){
    comp_raster<-'IBCSO_v1'
    r0<-raster(IBCSO1)
  }
  if (comp_raster == 'SRTM15+ v2.2'){
    comp_raster<-'SRTM15'
    r0<-raster(SRTM)
  }
  crs(r0)<-IBCSO_proj
  current_set<-subset(IBCSO_tiles,ID %in% current_tiles)
  area<-rgeos::gUnaryUnion(current_set)
  ex<-extent(area)
  
  r1<-crop(r_IBCSO2,area)
  r1<-mask(r1,area)
  r0<-crop(r0,area)
  r0<-mask(r0,area)
  r_mean<-(r0+r1)/2
  r_diff<-r1-r0
  r_discrepancy<-r_diff/r_mean
  crs(r_discrepancy)<-IBCSO_proj
  rgdal::writeOGR(current_set,dsn=file.path(RESDIR,paste0('AOI_',AOI,'_tiles.gpkg')),layer=AOI,driver='GPKG',overwrite_layer = T)
  rgdal::writeOGR(as(area,'SpatialPolygonsDataFrame'),dsn=file.path(RESDIR,paste0('AOI_',AOI,'_area.gpkg')),layer=AOI,driver='GPKG',overwrite_layer = T)
  
  raster::writeRaster(r0,file.path(RESDIR,paste0('AOI_',AOI,'_',comp_raster)),format='GTiff',overwrite=T)
  raster::writeRaster(r1,file.path(RESDIR,paste0('AOI_',AOI,'_IBCSO_v2')),format='GTiff',overwrite=T)
  raster::writeRaster(r_discrepancy,file.path(RESDIR,paste0('AOI_',AOI,'_discrepancy')),format='GTiff',overwrite=T)
}