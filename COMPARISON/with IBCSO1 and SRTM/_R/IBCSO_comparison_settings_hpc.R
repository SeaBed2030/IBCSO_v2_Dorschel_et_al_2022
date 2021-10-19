library(raster)
library(sp)
library(ggplot2)


IBCSO_proj<-sp::CRS("+proj=stere +ellps=WGS84 +lat_0=-90 +lon_0=0dE +lat_ts=-65")

colours=list(IBCSO_blue='#2c6996',IBCSO_orange='#FF5601',IBCSO_dark='#3A3A3A')

IBCSO_palette<-colorRampPalette(c(colours$IBCSO_blue, colours$IBCSO_orange))
ma_first<-100 #first ma window
ma_second<-5 #second ma_window (applied to first ma)

colors <- c(adjustcolor(colours$IBCSO_blue,.6),adjustcolor(colours$IBCSO_orange,.6))
names(colors)<-c(
  paste0("Moving average (",ma_first,' steps)'),paste0("Moving average (",ma_first*ma_second,' steps)')
  )

plot_theme<-ggplot2::theme_light()

files<-list(
	IBCSO1=file.path(PROJECTDIR,'DATA','IBCSO_v1_bed_only_ocean.tif'),
	IBCSO2_60=file.path(PROJECTDIR,'DATA','IBCSO_v2_bed_only_ocean_60deg.tif'),
	IBCSO2_50=file.path(PROJECTDIR,'DATA','IBCSO_v2_bed_only_ocean.tif'),
	SRTM=file.path(PROJECTDIR,'DATA','SRTM15plus_V2-2_only_ocean.tif')
)

moving_fn<-function(x,y,step=3,fun=mean,SORT=T){
  data<-cbind(x,y)
  data<-data[complete.cases(data),]
  if(SORT){
    data<-data[order(data[,1]),]
  }
  fn_data<-data.frame()
  print(nrow(data))
  for (bin in seq(1,nrow(data),step)){
    to_<-bin+step-1
    if(to_ > nrow(data)){
      to_<-nrow(data)
    }
    #line<-apply(data[bin:to_,],MARGIN=2,FUN=mean)
	x_<-mean(data[bin:to_,1])
	y_<-mean(data[bin:to_,2])
    #fn_data<-rbind(fn_data,data.frame(x_=line[1],y_=line[2],bin=bin))
    fn_data<-rbind(fn_data,data.frame(x_=x_,y_= y_,bin=bin))
  }
  return(fn_data)
}

moving_fn2<-function(x,y,step=3,fun=mean,SORT=T){
  data<-cbind(x,y)
  data<-data[complete.cases(data),]
  if(SORT){
    data<-data[order(data[,1]),]
  }
  x<-accelerometry::movingaves(data[,1],window=step)
  y<-accelerometry::movingaves(data[,2],window=step)
  fn_data<-data.frame(x_=x, y_=y)
  return(fn_data)
}
