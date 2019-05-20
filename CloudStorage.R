#==========================================================================
#==========================================================================
#' @title Find point data available in data.mol.org bucket
#' 
#' @description Pulls point data (shapefiles) in Google Cloud Storage and saves it to HPC
#'
#' @details
#' See Examples.
#'
#' @param 
#'
# @keywords
#' @export
#'
#' @return NULL
#' @author Erica Stuber <efstuber@@gmail.com> and Yanina Sica <yanina.sica@@gmail.com>
# @note
# @seealso
# @references
# @aliases - a list of additional topic names that will be mapped to
# this documentation when the user looks them up from the command
# line.
# @family - a family name. All functions that have the same family tag will be linked in the documentation.

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


#Get list of species to run
CarstenBigqueryLookup<-read.csv("/gpfs/ysm/project/ys628/SDMs/getData/africa_synonyms.csv")

########## Get other point data from data.mol.org bucket 

# point data in google cloud
shapefiles = gcs_list_objects(prefix='shapefiles/points/birds/',detail = c("summary"))
shapefiles = separate(shapefiles, name, into = c("path1", "path2","path3", "file"), sep = "/")
shapefiles = separate(shapefiles, file, into = c('name',"path4"), sep = '.zip')
shapefiles_name = shapefiles[,c('name')]

#species of interest
species = unique(CarstenBigqueryLookup$species_MOL)
species_=gsub(" ","_",species)
head(species_)
head(shapefiles_name)

#species that have point data
species_to_run = intersect(species_, shapefiles_name)
length(species_to_run)


input_dir= "shapefiles/points/birds/"
save_dir = "/gpfs/ysm/project/ys628/SDMs/getData/data.mol_shapefiles/"


for (i in unique(species_to_run)){
  print(i)
  species_zip[[i]] <- paste0(input_dir,i,".zip")
  save_zip[[i]] <- paste0(save_dir,i,".zip")
  gcs_get_object(object_name = species_zip[[i]], saveToDisk = save_zip[[i]])
}

#in bash: unzip '*.zip' to unzip all files in the directory
