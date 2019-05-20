
rm(list=ls())
library(rgdal)
library(stringr)

rangemaps_dir = "/gpfs/ysm/project/ys628/SDMs/getData/data.mol_rangemaps"
rangemaps_names = tools::file_path_sans_ext(list.files("/gpfs/ysm/project/ys628/SDMs/getData/data.mol_rangemaps/", pattern = "*.shp"))
env = raster::raster("/gpfs/ysm/project/ys628/SDMs/getData/Chelsa_AF_1_12_15_forest.tif")  ####environmental data for africa
myprj = raster::crs(env[[1]])
save_dir_proj = "/gpfs/ysm/project/ys628/SDMs/getData/data.mol_rangemaps/rangemaps_reproj"
save_dir_raster = "/gpfs/ysm/project/ys628/SDMs/getData/rangemap_data_rasterized"

for (i in 1:length(rangemaps_names)){
  species_shp = subset(rgdal::readOGR(dsn=rangemaps_dir, layer=rangemaps_names[i], verbose = FALSE), DATASET=="jetz_maps")
  print(rangemaps_names[i])
  species_shp_reproj = sp::spTransform(species_shp, CRSobj = myprj)#transform to same projection as environmental data
  save_filename = rangemaps_names[i]
  rgdal::writeOGR(species_shp_reproj, dsn=save_dir_proj, layer=save_filename, driver="ESRI Shapefile")
  print('shapefile projected and saved')
  tmp.raster=raster::raster(ncol = ncol(env[[1]]), nrow = nrow(env[[1]]),crs = raster::crs(species_shp_reproj))
  raster::extent(tmp.raster)=raster::extent(env[[1]])
  species_raster = raster::rasterize(species_shp_reproj, tmp.raster, field = species_shp_reproj@data$SCIENTIFIC)
  print('rasterized')
  save_raster = paste0(save_dir_raster,"/",save_filename)
  raster::writeRaster(species_raster, filename = save_raster, format = "GTiff", overwrite=TRUE)
  print('raster saved')
}

