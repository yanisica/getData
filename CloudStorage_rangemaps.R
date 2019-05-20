#==========================================================================
# Find range map data available in data.mol.org bucket
# 
# Pulls range maps (shapefiles) in Google Cloud Storage and saves it to HPC
# Pulls only range maps from the list of species that have point data 
#==========================================================================

rm(list=ls())

#install.packages('devtools') 
#devtools::install_github("rstats-db/bigrquery")
library(bigrquery)
library(readr)
library(DBI)
library(dplyr)
library(tidyr)
library(jsonlite) 
library(stringr)

#Autentication for BigQuery and GoogleCloud
GCP_SERVICE_KEY = "~/map-of-life-e38c8605def2-sdm-user.json" #key should always be in your home directory
Sys.setenv("GCS_AUTH_FILE" = GCP_SERVICE_KEY)
Sys.setenv("GCS_DEFAULT_BUCKET" = "data.mol.org") #buchet where shapefiles are
library(googleCloudStorageR)
bigrquery::set_service_token(GCP_SERVICE_KEY)

########## Get rangemap data from data.mol.org bucket 

# species range maps in google cloud
range_maps = gcs_list_objects(prefix='shapefiles/range/birds/',detail = c("summary"))
range_maps = separate(range_maps, name, into = c("path1", "path2","path3", "file"), sep = "/")
range_maps = separate(range_maps, file, into = c('name',"path4"), sep = '.zip')
range_maps_name = range_maps[,c('name')]

# species that have point data
load_BQ_dir="/gpfs/ysm/project/ys628/SDMs/getData/data_BigQuery/by_species/"
species_csv=unique(str_split(list.files(load_BQ_dir,pattern=".csv"),".csv",simplify=TRUE))[,1]

#species that have point data (ebird/gbif) + range maps
rangemaps_to_run = intersect(species_csv, range_maps_name)
length(rangemaps_to_run)
ls
input_dir= "shapefiles/range/birds/"
save_dir = "/gpfs/ysm/project/ys628/SDMs/getData/data.mol_rangemaps/"


for (i in unique(rangemaps_to_run)){
  print(i)
  species_zip[[i]] <- paste0(input_dir,i,".zip")
  save_zip[[i]] <- paste0(save_dir,i,".zip")
  gcs_get_object(object_name = species_zip[[i]], saveToDisk = save_zip[[i]])
}

# In bash unzip all files in save_dir
# unzip '*.zip' 