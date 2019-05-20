#==========================================================================
#==========================================================================
#' @title Find point data available in BigQuery_map-of-life (ebird + GBIF) using the synonyms table
#' 
#' @description Pulls point data form ebird and GBIF matching synonyms and saves it to HPC
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
Sys.setenv("GCS_DEFAULT_BUCKET" = "mol-playground") #bucket where BigQuery results will be placed
library(googleCloudStorageR)
bigrquery::set_service_token(GCP_SERVICE_KEY)


#Get list of species to run
CarstenBigqueryLookup<-read.csv("/gpfs/ysm/project/ys628/SDMs/getData/africa_synonyms.csv")


########## Get ebird and gbif data

#BigQuery connections
con = DBI::dbConnect(bigrquery::bigquery(), 
                     project = "map-of-life", 
                     dataset = "ebird", 
                     billing = "map-of-life")
con2 = DBI::dbConnect(bigrquery::bigquery(), 
                      project = "map-of-life", 
                      dataset = "gbif", 
                      billing = "map-of-life")

#DBI::dbListTables(con) #Check the tables to use

#make CarstenBigqueryLookup$bigquery_candidate into species list to query
syn = CarstenBigqueryLookup$bigquery_candidate

########ebird query
ebird_query_result = bigrquery::bq_project_query(
  x = con@project,
  query = paste(
    "SELECT scientific_name, latitude, longitude, observation_date",
    "FROM `map-of-life.ebird.ebird_mol_201808`",
    "WHERE scientific_name IN (", paste0("'", syn, "'", collapse = ", "), ")"
  )
)

#Place results in mol-playground bucket (BigQuery generated several csv files because size)
bigrquery::bq_table_save(ebird_query_result,destination_uris = "gs://mol-playground/yani/ebirdBQ/ebird_bigquery_201808-*.csv")

#Move results to HPC
ebird_data = gcs_list_objects(prefix='yani/ebirdBQ/',detail = c("summary"))

length(ebird_data$name)
ebird_data_filenames=separate(ebird_data, name,into = c("path1", "path2","file"), sep = "/")

save_dir = "/gpfs/ysm/project/ys628/SDMs/getData/data_BigQuery/ebird/"

for (i in 2:length(ebird_data$name)){
 save_filename[[i]] = paste0(save_dir,ebird_data_filenames$file[[i]])
 save_file[[i]] =ebird_data$name[[i]]
 gcs_get_object(object_name = save_file[[i]], saveToDisk = save_filename[[i]])
 }


#######gbif query
gbif_query_result = bigrquery::bq_project_query(
  x = con2@project,
  query = paste(
    "SELECT species, latitude, longitude, eventDate, issue",
    "FROM `map-of-life.gbif.gbif_mol_201810`",
    "WHERE species IN (", paste0("'", syn, "'", collapse = ", "), ")"
  )
)

#Place results in mol-playground bucket (BigQuery generated several csv files because of size)
bigrquery::bq_table_save(gbif_query_result,destination_uris = "gs://mol-playground/yani/gbifBQ/gbif_bigquery_201808-*.csv")

#Move results to HPC
gbif_data = gcs_list_objects(prefix='yani/gbifBQ/',detail = c("summary"))

length(gbif_data$name)
gbif_data_filenames=separate(gbif_data, name,into = c("path1", "path2","file"), sep = "/")

save_dir = "/gpfs/ysm/project/ys628/SDMs/getData/data_BigQuery/gbif/"

for (i in 2:length(gbif_data$name)){
  save_filenames[[i]] <- paste0(save_dir,gbif_data_filenames$file[[i]])
  save_file[[i]] <- gbif_data$name[[i]]
  gcs_get_object(object_name = save_file[[i]], saveToDisk = save_filenames[[i]])
}


########## Read BigQuery data and subset to single species
## This part of the workflow could be ran as a batch process (use code in point_data_by_species.R)...set working dir 'home'
## In an interactive session, it took 8hs for ~2500 species in ~200 jsonfiles

#Get list of species 
#CarstenBigqueryLookup<-read.csv("/gpfs/ysm/project/ys628/SDMs/getData/africa_synonyms.csv")

input_jsonfiles_gbif <- list.files('/gpfs/ysm/project/ys628/SDMs/getData/data_BigQuery/gbif', pattern='*.csv', full.names = TRUE)
input_jsonfiles_ebird <- list.files('/gpfs/ysm/project/ys628/SDMs/getData/data_BigQuery/ebird-redo', pattern='*.csv', full.names = TRUE)
output_species_QC = "/gpfs/ysm/project/ys628/SDMs/getData/data_BigQuery/by_species/to_QC/"

start <- Sys.time()
for (i in input_jsonfiles_gbif){
  print (i)
  temp = jsonlite::stream_in(file(i))
  temp = dplyr::left_join(temp,CarstenBigqueryLookup,by=c("species"="bigquery_candidate")) 
  temp = temp[,c("species_MOL","latitude","longitude","eventDate")];names(temp)=c("species_MOL","latitude","longitude","observation_date")
  temp = temp[complete.cases(temp[,1:4]),]#remove NAs
  temp$observation_date<-as.Date(temp$observation_date, format="%Y-%m-%d")
  temp = temp[temp$observation_date>"1970-01-01",]#keep data from 1970 only
  temp$jsonfilename = i #useful for QC
  #print(temp)
  
  for (j in unique(temp$species_MOL)){
    print(j)
    species_csv = temp[which(temp$species_MOL==j),] #subset to single species
    species_MOL_=gsub(" ","_",j)
    save_filename_QC=paste0(output_species_QC,species_MOL_,".csv")
    readr::write_csv(species_csv, path = save_filename_QC, append=TRUE)
  }
}

for (i in input_jsonfiles_ebird){
  print (i)
  temp = jsonlite::stream_in(file(i))
  temp = dplyr::left_join(temp,CarstenBigqueryLookup,by=c("scientific_name"="bigquery_candidate")) 
  temp = temp[,c("species_MOL","latitude","longitude","observation_date")]
  temp = temp[complete.cases(temp[,1:4]),]#remove NAs
  temp$observation_date<-as.Date(temp$observation_date, format="%Y-%m-%d")
  temp = temp[temp$observation_date>"1970-01-01",]#keep data from 1970 only
  temp$jsonfilename = i #useful for QC
  #print(temp)
  
  for (j in unique(temp$species_MOL)){
    print(j)
    species_csv = temp[which(temp$species_MOL==j),] #subset to single species
    species_MOL_=gsub(" ","_",j)
    save_filename_QC=paste0(output_species_QC,species_MOL_,".csv")
    readr::write_csv(species_csv, path = save_filename_QC, append=TRUE)
    
  }
}
print(Sys.time() - start)

# Remove duplicates and 0s
input_species <- list.files('/gpfs/ysm/project/ys628/SDMs/getData/data_BigQuery/by_species/to_QC/', pattern='*.csv', full.names = FALSE)
output_species <- "/gpfs/ysm/project/ys628/SDMs/getData/data_BigQuery/by_species/"

for (i in input_species){
  input_filename = paste0(output_species_QC,i)
  species = read_csv(input_filename, col_names = c('species_MOL','lat','long','observation_date','jsonfilename'),
                     col_types = cols(col_character(), col_number(), col_number(), col_date(), col_character()))
  print(i)
  species = unique(species)#remove duplicates
  species = species[,2:3]#keep lat/lon
  species_0 = apply(species,1, function(row) all(row!=0))
  species = species[species_0,]#remove 0 from lat/lon
  save_filename = paste0(output_species,i)
  readr::write_csv(species, path = save_filename, append=FALSE)
}
