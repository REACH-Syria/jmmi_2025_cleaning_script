#'---------------------------------------------------------------------------------------
# Authors: Evelyn G.----
#'---------------------------------------------------------------------------------------

#'---------------------------------------------------------------------------------------
# prepare_environment ----
#'---------------------------------------------------------------------------------------
rm(list = ls())
library(rstudioapi)
setwd(dirname(getActiveDocumentContext()$path))
getwd()

if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")

pacman::p_load(
  tidyverse, readxl, writexl, openxlsx, httr, gsheet, gridExtra,
  magrittr, sf, leaflet, mapview, anytime, lubridate, data.table,
  cleaningtools, Hmisc, rstatix
)

# cleaning functions
source("functions/cleaning_functions.R")
# Directories
create_directories("outputs")

#'---------------------------------------------------------------------------------------
# Loading the tool and data  ----
#'---------------------------------------------------------------------------------------
filename.dataset <- "inputs/REACH_SYR_SYR1702_JMMI_March25.xlsx"
koboToolPath <-"inputs/TOOL_JMMI_MARCH.xlsx"
data_tool <- load_data_and_tool(filename.dataset, koboToolPath)


raw_df <- data_tool$raw
survey <- data_tool$tool_survey
choices <- data_tool$tool_choices

# cleaning functions
source("functions/cleaning_functions.R")


#'---------------------------------------------------------------------------------------
# Other responses ----
#'---------------------------------------------------------------------------------------
output <- cleaningtools::check_others(
  dataset = raw_df,
  uuid_column = "X_uuid",
  columns_to_check = names(raw_df %>%
                             dplyr::select(starts_with("SYR_note_")))
)

other_log <- output$other_log

other_log <- other_log %>% 
  mutate(unique_id = paste(uuid,question, sep = "_"),
         checked = case_when(unique_id %in% others_bind$unique_id ~ "Yes",
                             TRUE ~ "No")) %>% 
  dplyr::filter(checked == "No")


other_log_translated <- other_log %>%
  mutate(old_value_en=translateR::translate(content.vec = old_value ,
                                           microsoft.api.key = source("resources/microsoft.api.key.syria.R")$value,
                                           microsoft.api.region = "switzerlandnorth",
                                           source.lang="ar", target.lang="en"))

other_log_translated <- other_log_translated %>% 
  mutate(
    true_other = NA_character_,
    existing_other = NA_character_,
    invalid_other = NA_character_
  )

write.xlsx(other_log_translated, file = paste0("outputs/cleaning_logs/", format(Sys.time(),"%Y_%m_%d_%H%M"), "_other_responses.xlsx"))
 
#'---------------------------------------------------------------------------------------
# Currency conversion ----
#'---------------------------------------------------------------------------------------

converted_data <- currency_conversion(raw_df)
converted_nes <- converted_data$Converted_NES_SRP
converted_nws <- converted_data$Converted_NWS_TRY
df_currency <- converted_data$Data_NES_SRP_NWS_TRY

converted_data %>%
  openxlsx::write.xlsx(
    .,
    file = paste0("outputs/raw_data_converted/", format(Sys.time(),"%Y_%m_%d_%H%M"), "JMMJ_converted_raw_data.xlsx")
    
    
  )
#'---------------------------------------------------------------------------------------
# Checking Outliers of the prices collected----
#'---------------------------------------------------------------------------------------
check_list <- read_excel("inputs/check_list.xlsx")

### Fuel, water and Internet prices ----
table_dft <- outliers_fuel_water(df_currency, raw_df)
table_dft_west <- table_dft %>% filter(region == "Northwest")
table_dft_east <- table_dft %>% filter(region == "Northeast")

### Northwest Other items collected ----
df_counts <- df_currency %>%
  filter(region == "Northwest")

df_check_northwest <- df_counts %>%
  check_logical_with_list(uuid_column = "X_uuid",
                          list_of_check = check_list,
                          check_id_column = "check_id",
                          check_to_perform_column = "check",
                          columns_to_clean_column = "variables_to_clean",
                          description_column = "description")


df_check_northwest_list <- create_combined_log(df_check_northwest) %>%
  add_info_to_cleaning_log(dataset_uuid_column = "X_uuid",
                           cleaning_log_uuid_column = "uuid",
                           information_to_add = c("region","organisation","deviceid",
                                                  "enumerator","admin4_label","shop_currency"))

# adding notes
df_check_northwest_list <- clean_log_with_notes(df_check_northwest_list)
northwest_log_notes <- df_check_northwest_list$cleaning_log
# always review this
df_check_northwest_list_df <- df_check_northwest_list %>%
  cleaningtools::create_xlsx_cleaning_log(output_path = paste0("outputs/cleaning_logs/northwest/logical_outlier_checks_northwest.xlsx"),
                                          change_type_col = "change_type",
                                          cleaning_log_name = "cleaning_log",
                                          kobo_survey = survey,
                                          kobo_choices = choices,
                                          sm_dropdown_type = "logical",
                                          use_dropdown = TRUE)

df_check_northwest_list <- clean_log_no_notes(df_check_northwest_list)
df_check_northwest_list$cleaning_log <- plyr::rbind.fill(df_check_northwest_list$cleaning_log, table_dft_west)
df_check_northwest_list$cleaning_log <- left_join(df_check_northwest_list$cleaning_log, df_translation, by = "question")
df_check_northwest_list$cleaning_log <- df_check_northwest_list$cleaning_log %>%
  mutate(explanation = " ") %>%
  relocate("issue",explanation,"old_value","new_value","change_type", .after = "label::arabic") %>% 
  mutate(unique_id = paste(uuid,question, sep = "_"),
         checked = case_when(unique_id %in% log_nws$unique_id ~ "Yes",
                             TRUE ~ "No")) 
# %>% 
#   dplyr::filter(checked == "No")


#Saving the cleaning logs for each organisation in northwest
library(dplyr)

org_list <- unique(df_check_northwest_list$cleaning_log$organisation)
unique(org_list)
# Iterate over each organization
for(org in org_list) {
  data_check_2 <- df_check_northwest_list
  
  data_check_2$cleaning_log <- data_check_2$cleaning_log %>%
    filter(organisation == org)
  
  if (nrow(data_check_2$cleaning_log) > 0) {
    output_dir <- "outputs/cleaning_logs/northwest/"
    if (!dir.exists(output_dir)) {
      dir.create(output_dir, recursive = TRUE)
    }
    
    cleaningtools::create_combined_log(data_check_2) %>%
      cleaningtools::create_xlsx_cleaning_log(output_path = paste0(output_dir, "northwest_data_checks_", org, ".xlsx"),
                                              change_type_col = "change_type",
                                              cleaning_log_name = "cleaning_log",
                                              kobo_survey = survey,
                                              kobo_choices = choices,
                                              sm_dropdown_type = "logical",
                                              use_dropdown = TRUE)
  } else {
    message(paste("No data for organization:", org))
  }
}

### Northeast Other items collected ----
df_counts <- df_currency %>%
  filter(region == "Northeast")

df_check_northeast <- df_counts %>%
  check_logical_with_list(uuid_column = "X_uuid",
                          list_of_check = check_list,
                          check_id_column = "check_id",
                          check_to_perform_column = "check",
                          columns_to_clean_column = "variables_to_clean",
                          description_column = "description")


df_check_northeast_list <- create_combined_log(df_check_northeast) %>%
  add_info_to_cleaning_log(dataset_uuid_column = "X_uuid",
                           cleaning_log_uuid_column = "uuid",
                           information_to_add = c("region","organisation","deviceid",
                                                  "enumerator","admin4_label","shop_currency"))

# adding notes
df_check_northeast_list <- clean_log_with_notes(df_check_northeast_list)
northeast_log_notes <- df_check_northeast_list$cleaning_log
# always review this
df_check_northeast_list_df <- df_check_northeast_list %>%
  cleaningtools::create_xlsx_cleaning_log(output_path = paste0("outputs/cleaning_logs/northeast/logical_outlier_checks_northeast.xlsx"),
                                          change_type_col = "change_type",
                                          cleaning_log_name = "cleaning_log",
                                          kobo_survey = survey,
                                          kobo_choices = choices,
                                          sm_dropdown_type = "logical",
                                          use_dropdown = TRUE)

df_check_northeast_list <- clean_log_no_notes(df_check_northeast_list)
df_check_northeast_list$cleaning_log <- plyr::rbind.fill(df_check_northeast_list$cleaning_log, table_dft_east)
df_check_northeast_list$cleaning_log <- left_join(df_check_northeast_list$cleaning_log, df_translation, by = "question")
df_check_northeast_list$cleaning_log <- df_check_northeast_list$cleaning_log %>%
  mutate(explanation = " ") %>%
  relocate("issue",explanation,"old_value","new_value","change_type", .after = "label::arabic") %>% 
  mutate(unique_id = paste(uuid,question, sep = "_"),
         checked = case_when(unique_id %in% log_nes$unique_id ~ "Yes",
                             TRUE ~ "No")) 
# %>% 
#   dplyr::filter(checked == "No")


#Saving the cleaning logs for each organisation in northeast

org_list <- unique(df_check_northeast_list$cleaning_log$organisation)
print(org_list)
for(org in org_list) {
  data_check_2 <- df_check_northeast_list
  
  data_check_2$cleaning_log <- data_check_2$cleaning_log %>%
    filter(organisation == org)
  
  if (nrow(data_check_2$cleaning_log) > 0) {
    output_dir <- "outputs/cleaning_logs/northeast/"
    if (!dir.exists(output_dir)) {
      dir.create(output_dir, recursive = TRUE)
    }
    
    create_combined_log(data_check_2) %>%
      create_xlsx_cleaning_log(output_path = paste0(output_dir, "northeast_data_checks_", org, ".xlsx"),
                               change_type_col = "change_type",
                               cleaning_log_name = "cleaning_log",
                               kobo_survey = survey,
                               kobo_choices = choices,
                               sm_dropdown_type = "logical",
                               use_dropdown = TRUE)
  } else {
    message(paste("No data for organization:", org))
  }
}

### Saving the files ----

northwest_outliers <- df_check_northwest_list$cleaning_log
northeast_outliers <- df_check_northeast_list$cleaning_log
all_outliers <- rbind(northwest_outliers,northeast_outliers)
all_outliers_notes <- rbind(northeast_log_notes, northwest_log_notes)

write.xlsx(all_outliers, file = "outputs/cleaning_logs/outliers_log_checked_2025_03_16.xlsx")
write.xlsx(all_outliers_notes, file = "outputs/cleaning_logs/all_outliers_checked_with_notes_2025_03_16.xlsx")
