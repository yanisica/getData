#==========================================================================
#==========================================================================
#==========================================================================
#' @title Find point data available in BigQuery_map-of-life (ebird + GBIF) using the synonyms table
#' 
#' @description Pulls point data form ebird and GBIF matching synonyms 
#'
#' @details
#' See Examples.
#'
#' @param 
#'
# @keywords
#' @export
#'
# @examples
#'#'
#' @return NULL
#' @author Erica Stuber <efstuber@@gmail.com>
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

#Autentication for BigQuery and GoogleCloud
GCP_SERVICE_KEY = "~/map-of-life-e38c8605def2-sdm-user.json" #key should always be in your home directory
Sys.setenv("GCS_AUTH_FILE" = GCP_SERVICE_KEY)
Sys.setenv("GCS_DEFAULT_BUCKET" = "mol-playground")
library(googleCloudStorageR)
bigrquery::set_service_token(GCP_SERVICE_KEY)

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

#Get list of species 
CarstenBigqueryLookup<-read.csv("/gpfs/ysm/project/ys628/SDMs/getDataBQ/africa_synonyms_test2.csv")

#make CarstenBigqueryLookup$bigquery_candidate into species list to query
syn = CarstenBigqueryLookup$bigquery_candidate

########## Query MOL point data records with the list of species
###ebird
ebird_query_result = bigrquery::bq_project_query(
  x = con@project,
  query = paste(
    "SELECT scientific_name, latitude, longitude, observation_date",
    "FROM `map-of-life.ebird.ebird_mol_201808`",
    "WHERE scientific_name IN (", paste0("'", syn, "'", collapse = ", "), ")"
  )
)

#Place results in bucket
bigrquery::bq_table_save(ebird_query_result,destination_uris = "gs://mol-playground/yani/ebird_bigquery_201808_test2.csv")
#Move results to HPC
gcs_get_object("yani/ebird_bigquery_201808_test2.csv", saveToDisk = "/gpfs/ysm/project/ys628/SDMs/bigquery/ebird_bigquery_201808_test2.csv")

#######gbif
gbif_query_result = bigrquery::bq_project_query(
  x = con2@project,
  query = paste(
    "SELECT species, latitude, longitude, eventDate, issue",
    "FROM `map-of-life.gbif.gbif_mol_201810`",
    "WHERE species IN (", paste0("'", syn, "'", collapse = ", "), ")"
  )
)

#Place results in bucket
bigrquery::bq_table_save(gbif_query_result,destination_uris = "gs://mol-playground/yan/gbif_bigquery_201808_test2.csv")
#Move results to HPC
gcs_get_object("yani/gbif_bigquery_201808_test2.csv", saveToDisk = "/gpfs/ysm/project/ys628/SDMs/bigquery/gbif_bigquery_201808_test2.csv")

########## Read query data and clean it
#read data
ebird_bigquery <- jsonlite::stream_in(file('/gpfs/ysm/project/ys628/SDMs/getDataBQ/ebird_bigquery_201808_test2.csv')) 
#ebird_query_df=as.data.frame(ebird_bigquery)#generate a dataframe with the results of the query
ebird_query_df_join=dplyr::left_join(ebird_bigquery,CarstenBigqueryLookup,by=c("scientific_name"="bigquery_candidate")) 

gbif_bigquery <- jsonlite::stream_in(file('/gpfs/ysm/project/ys628/SDMs/getDataBQ/gbif_bigquery_201808_test2.csv')) 
#ebird_query_df=as.data.frame(ebird_bigquery)#generate a dataframe with the results of the query
gbif_query_df_join=dplyr::left_join(gbif_bigquery,CarstenBigqueryLookup,by=c("species"="bigquery_candidate")) 

#retain data from 1970 
ebird=ebird_query_df_join[,c("species_MOL","latitude","longitude","observation_date")]
gbif=gbif_query_df_join[,c("species_MOL","latitude","longitude","eventDate")];names(gbif)=c("species_MOL","latitude","longitude","observation_date")
point_obs<-rbind(ebird,gbif)
point_obs<-point_obs[complete.cases(point_obs[,1:4]),]
point_obs$observation_date<-as.Date(point_obs$observation_date, format="%Y-%m-%d")
point_obs<-point_obs[point_obs$observation_date>"1970-01-01",]#retain data from 1970 only

#get point data for each species separately
species_MOL=unique(point_obs$species_MOL)
species_MOL_=gsub(" ","_",species_MOL)
save_dir<-"/gpfs/ysm/project/ys628/SDMs/getDataBQ/data_species/"

for (i in 1:length(species_MOL)){
  
  print(i)
  print(species_MOL[i])
  species_csv <-point_obs[point_obs$species_MOL==species_MOL[i],] #subset to single species
  species_csv<-unique(species_csv)#remove duplicate records
  species_csv<-species_csv[,2:3]##keep lat long only
  names(species_csv)<-c("lat","lon")
  save_filename=paste0(save_dir,species_MOL_[i],".csv")
  readr::write_csv(species_csv, path = save_filename)
}