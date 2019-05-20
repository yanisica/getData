rm(list=ls())
library(rgdal)
library(stringr)

load_CS_dir="/gpfs/ysm/project/ys628/SDMs/getData/data.mol_shapefiles"
load_BQ_dir="/gpfs/ysm/project/ys628/SDMs/getData/data_BigQuery/by_species/"
save_dir="/gpfs/ysm/project/ys628/SDMs/getData/point_data_combined/"

##for all species files
species_shapefiles=list.files(load_CS_dir,pattern=".shp",full.names = TRUE)
species_shp=unique(str_split(list.files(load_CS_dir,pattern=".shp"),".shp",simplify=TRUE))[,1]
length(species_shp)
species_csvfiles=list.files(load_BQ_dir,full.names = TRUE)
species_csv=unique(str_split(list.files(load_BQ_dir,pattern=".csv"),".csv",simplify=TRUE))[,1]
length(species_csv)

for (i in 1:length(species_csv)){
  print(i)
  print(species_csv[i])
  if(length(list.files(load_CS_dir,pattern = glob2rx(paste0(species_csv[i], ".shp"))) %in% list.files(load_CS_dir))>0){
    sppshp_file<-list.files(load_CS_dir,pattern = glob2rx(paste0(species_csv[i], ".shp")),full.names = TRUE)
    sppshp<-subset(readOGR(dsn=load_CS_dir, layer=species_shp[i], verbose = FALSE),!DATASET %in% c("ebird","gbif"))
    sppshp@data$lon<-sppshp@coords[,1]
    sppshp@data$lat<-sppshp@coords[,2]
    print(table(sppshp@data$DATASET))
    sppshp@data<-sppshp@data[complete.cases(sppshp@data[,c(1,2,3,6,7)]),] #has date info
    sppshp@data$DATE<-as.Date(sppshp$DATE, format="%Y/%m/%d")
    sppshp@data<-sppshp@data[sppshp@data$DATE>"1970-01-01",] #keep after 1970
    sppshp@data<-unique(sppshp@data[,6:7])
    sppcsv<-as.data.frame(sppshp@data[,c(2,1)])
  } else{sppcsv=NULL}

  if(length(list.files(load_BQ_dir,pattern = glob2rx(paste0(species_csv[i], ".csv"))) %in% list.files(load_BQ_dir))>0){
    existing_file=list.files(load_BQ_dir,pattern = glob2rx(paste0(species_csv[i], ".csv")),full.names = TRUE)
    existing_points=read.csv(existing_file)
  } else{existing_points=NULL}

  names (existing_points) <- names(sppcsv)
  full_points=rbind(existing_points,sppcsv)
  write.csv(full_points, file = paste0(save_dir,species_csv[i],".csv"),row.names = FALSE)
  
}
