rm(list=ls())
PROJECTDIR<-'/isibhv/projects/seabed2030/IBCSO2_comp'
setwd(PROJECTDIR)
source(file.path(PROJECTDIR,'IBCSO_comparison_settings_hpc.R'))

ma <- read.csv(file.path(PROJECTDIR,'DATA','COMP_IBCSO1_data_ma_a.csv'),sep='\t',header=T,dec='.')
ma2 <- read.csv(file.path(PROJECTDIR,'DATA','COMP_IBCSO1_data_ma_b.csv'),sep='\t',header=T,dec='.')

p<-ggplot(data=ma, aes(y=y_diff,x=x_))
p<-p+geom_abline(intercept=0,slope=0,col='green')
p<-p+geom_line(col=adjustcolor(colours$IBCSO_blue,.6))
p<-p+geom_line(data=ma2, aes(y=y_diff,x=x_),col=adjustcolor(colours$IBCSO_orange,1),show.legend=F)
p<-p+ylab('Difference compared to IBCSO v1 Depth [m]')+xlab('IBCSO v2 Depth [m]')
p<-p+scale_color_manual("",values = colors)
p<-p+scale_y_continuous(breaks = seq(-6000, 6000, 250))
p<-p+scale_x_continuous(breaks = seq(-10000, 2000, 1000))
p<-p+plot_theme

rm(ma)
rm(ma2)

png(file.path(PROJECTDIR,"fig 6a.png"),3200,3200)
print(p)
graphics.off()

rm(p)


