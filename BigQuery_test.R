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
CarstenBigqueryLookup<-read.csv("/gpfs/ysm/project/ys628/SDMs/bigquery/africa_synonyms_test2.csv")

#make CarstenBigqueryLookup$bigquery_candidate into species list to query
syn = CarstenBigqueryLookup$bigquery_candidate

#### Query MOL point data records with the list of species
#######ebird
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

#read data
ebird_bigquery -> read.csv("/gpfs/ysm/project/ys628/SDMs/bigquery/africa_synonyms_test2.csv")
#mydata <- jsonlite::stream_in(file('/gpfs/loomis/project/fas/jetz/data/erica/BigQuery/erica_carsten_gbif_table_save.csv')) I DONT THINK I NEED THIS
#ebird_query_df=as.data.frame(ebird_bigquery)#generate a dataframe with the results of the query
ebird_query_df_join=dplyr::left_join(ebird_bigquery,CarstenBigqueryLookup,by=c("scientific_name"="Synonym")) 


gbif_bigquery -> read.csv("/gpfs/ysm/project/ys628/SDMs/bigquery/africa_synonyms_test2.csv")
gbif_query_df_join=dplyr::left_join(gbif_bigquery,CarstenBigqueryLookup,by=c("species"="Synonym")) 


