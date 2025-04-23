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
  cleaningtools, Hmisc, rstatix, janitor,jsonlite,dplyr
)

# cleaning functions
source("functions/cleaning_functions.R")
source("functions/calculations.R")
# Directories
create_directories("outputs")

#'---------------------------------------------------------------------------------------
# 1. Loading the tool and data  ----
#'---------------------------------------------------------------------------------------
filename.dataset <- "inputs/REACH_SYR_SYR1702_JMMI_April_2025_20.xlsx"
koboToolPath <-"inputs/TOOL_JMMI_April_2025.xlsx"
data_tool <- load_data_and_tool(filename.dataset, koboToolPath)


raw_df <- data_tool$raw
survey <- data_tool$tool_survey
choices <- data_tool$tool_choices

# adding the geocodes
geocodes_df<- read_excel("inputs/geo_codes/SYR_REACH_admin4_February2024.xlsx") %>% 
  select(Location_Pcode,LocationName_en,LocationName_ar)

raw_df <- raw_df %>% 
  left_join(geocodes_df, by = c("admin4_label" = "Location_Pcode"))
raw_df$LocationName_en
table(is.na(raw_df$LocationName_en))

write.xlsx(raw_df, file = paste0("outputs/enumerator_checks//", format(Sys.time(),"%Y_%m_%d_%H%M"), "_raw_data.xlsx"))
#'---------------------------------------------------------------------------------------
# 2. Other responses ----
#'---------------------------------------------------------------------------------------
output <- cleaningtools::check_others(
  dataset = raw_df,
  uuid_column = "X_uuid",
  columns_to_check = names(raw_df %>%
                             dplyr::select(starts_with("SYR_note_")))
)

other_log <- output$other_log

other_log <- other_log %>%
  mutate(unique_id = paste(uuid,question, sep = "_"))


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

write.xlsx(other_log_translated, file = paste0("outputs/cleaning_logs/translation/", format(Sys.time(),"%Y_%m_%d_%H%M"), "_other_responses.xlsx"))

#'---------------------------------------------------------------------------------------
# 3. Currency conversion ----
#'---------------------------------------------------------------------------------------

converted_data <- currency_conversion(raw_df)
converted_nes <- converted_data$Converted_NES_SRP
converted_nws <- converted_data$Converted_NWS_TRY
converted_scs <- converted_data$Converted_SCS_SRP
df_currency <- converted_data$Data_NES_SRP_NWS_TRY


converted_data %>%
  openxlsx::write.xlsx(
    .,
    file = paste0("outputs/raw_data_converted/", format(Sys.time(),"%Y_%m_%d_%H%M"), "JMMJ_converted_raw_data.xlsx")
    
    
  )

#'---------------------------------------------------------------------------------------
# 3A. Count of Prices ----
#'---------------------------------------------------------------------------------------

variables_to_aggregate <- names(fromJSON("./functions/external_to_internal_mapping.json"))
variables_to_impute <- variables_to_aggregate[!grepl("^price_smeb_", variables_to_aggregate)]

# Remove unwanted variables and add new ones
variables_to_impute <- setdiff(variables_to_impute, c("lpg_litre_price_item", "water_liter_price_item"))
variables_to_impute <- union(variables_to_impute, c("lpg_subsidised_price_item", "lpg_price_item", "water_truck_liter_price_unit_item"))

low_count_items_grouped <- function(data, item_list, group_var, org_var,enumerator_id, threshold = 3) {
  data %>%
    select(all_of(c(group_var, org_var, enumerator_id, item_list))) %>%
    group_by(across(all_of(c(group_var, org_var,enumerator_id)))) %>%
    summarise(across(all_of(item_list), ~ sum(!is.na(.)), .names = "count_{.col}"), .groups = "drop") %>%
    pivot_longer(
      cols = starts_with("count_"),
      names_to = "variable",
      names_prefix = "count_",
      values_to = "non_missing_count"
    ) %>%
    filter(non_missing_count > 0 & non_missing_count < threshold) %>%
    rename(group_value = !!group_var, org_name = !!org_var, enumerator_id= !! enumerator_id)
}


NES_low_counts <- low_count_items_grouped(converted_nes, variables_to_impute, group_var = "admin4_label", 
                                          org_var = "organisation", enumerator_id = "enumerator") %>% 
  # filter out the variable contain the name "_subsidised"
  filter(!grepl("_subsidised", variable)) %>% 
  left_join(geocodes_df, by = c("group_value" = "Location_Pcode"))

NWS_low_counts <- low_count_items_grouped(converted_nws, variables_to_impute, group_var = "admin4_label",
                                          org_var = "organisation", enumerator_id = "enumerator") %>% 
  # filter out the variable contain the name "_subsidised"
  filter(!grepl("_subsidised", variable)) %>% 
  left_join(geocodes_df, by = c("group_value" = "Location_Pcode"))

#low count South Central
SCS_low_counts <- low_count_items_grouped(converted_scs, variables_to_impute, group_var = "admin4_label",
                                          org_var = "organisation", enumerator_id = "enumerator") %>% 
  # filter out the variable contain the name "_subsidised"
  filter(!grepl("_subsidised", variable)) %>% 
  left_join(geocodes_df, by = c("group_value" = "Location_Pcode"))

write.xlsx(
  NES_low_counts,
  file = paste0("outputs/cleaning_logs/low_counts/", format(Sys.time(), "%Y_%m_%d_%H%M"), "_low_counts_NES.xlsx")
)

write.xlsx(
  NWS_low_counts,
  file = paste0("outputs/cleaning_logs/low_counts/", format(Sys.time(), "%Y_%m_%d_%H%M"), "_low_counts_NWS.xlsx")
)

# saving the south centrafile
write.xlsx(
  SCS_low_counts,
  file = paste0("outputs/cleaning_logs/low_counts/", format(Sys.time(), "%Y_%m_%d_%H%M"), "_low_counts_SCS.xlsx")
)


#'---------------------------------------------------------------------------------------
# Checking Organisation Names before data cleaning----
#'---------------------------------------------------------------------------------------

filled_org <- read_excel("inputs/org_issues.xlsx")

# reviewing the cleaning logs

cleaningtools::review_cleaning_log( raw_dataset = df_currency,
                                    raw_data_uuid_column = "X_uuid",
                                    cleaning_log = filled_org,
                                    cleaning_log_change_type_column = "change_type",
                                    change_response_value = "change_response",
                                    cleaning_log_question_column = "question",
                                    cleaning_log_uuid_column = "uuid",
                                    cleaning_log_new_value_column = "new_value")



# function for actually doing data cleaning
df_currency_org <- create_clean_data(raw_dataset = df_currency, 
                                     raw_data_uuid_column = 'X_uuid', 
                                     cl = filled_org,
                                     cleaning_log_change_type_column = "change_type",
                                     change_response_value = "change_response", 
                                     NA_response_value = "blank_response", 
                                     no_change_value = "no_action", 
                                     remove_survey_value = "remove_survey",
                                     cleaning_log_question_column =  "question",
                                     cleaning_log_uuid_column = 'uuid',
                                     cleaning_log_new_value_column = "new_value")

df_currency_org <- df_currency_org %>% 
  mutate(organisation = case_when(organisation == "Other" ~ organisation_name_label,
                                  TRUE ~ organisation)) %>%
  mutate(organisation = ifelse(organisation %in% c("Irc","irc") , "IRC", organisation),
         organisation = ifelse(organisation %in% c("Bahar Organization","بهار","BAHAR ORGANIZATION",
                                                   "BAHAR Organization") , "Bahar", organisation),
         organisation = ifelse(organisation %in% c("Takaful Al sham","Tas", "تكافل الشام",
                                                   "Tss") , "TAS", organisation)
  )
write.xlsx(
  df_currency_org,
  file = paste0("outputs/enumerator_checks//", format(Sys.time(), "%Y_%m_%d_%H%M"), "data_organisation_names.xlsx")
)
#'---------------------------------------------------------------------------------------
# 4. Checking Outliers of the prices collected----
#'---------------------------------------------------------------------------------------
check_list <- read_excel("inputs/check_list.xlsx")


df_translation <- survey %>%
  dplyr::select(`label::arabic`,name)%>%
  na.omit() %>%
  dplyr::rename("question" = name)


### Fuel, water and Internet prices ----
table_dft <- outliers_fuel_water(df_currency_org, raw_df)
table_dft_west <- table_dft %>% filter(region == "Northwest")
table_dft_east <- table_dft %>% filter(region == "Northeast")
table_dft_scs <- table_dft %>% filter(region == "Central South")

unique(df_currency_org$organisation)
### Northwest Other items collected ----
df_counts <- df_currency_org %>%
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
                                                  "enumerator","admin4_label","LocationName_ar","LocationName_en","shop_currency"))

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
  mutate(unique_id = paste(uuid,question, sep = "_"))



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
df_counts <- df_currency_org %>%
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
                                                  "enumerator","admin4_label","LocationName_ar","LocationName_en","shop_currency"))

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
  mutate(unique_id = paste(uuid,question, sep = "_")) 



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
# Central South

### Central South Other items collected ----
df_counts <- df_currency %>%
  filter(region == "Central South")

df_check_Central_South <- df_counts %>%
  check_logical_with_list(uuid_column = "X_uuid",
                          list_of_check = check_list,
                          check_id_column = "check_id",
                          check_to_perform_column = "check",
                          columns_to_clean_column = "variables_to_clean",
                          description_column = "description")


df_check_Central_South_list <- create_combined_log(df_check_Central_South) %>%
  add_info_to_cleaning_log(dataset_uuid_column = "X_uuid",
                           cleaning_log_uuid_column = "uuid",
                           information_to_add = c("region","organisation","deviceid",
                                                  "enumerator","admin4_label","LocationName_ar","LocationName_en","shop_currency"))

# adding notes
df_check_Central_South_list <- clean_log_with_notes(df_check_Central_South_list)
Central_South_log_notes <- df_check_Central_South_list$cleaning_log
# always review this
df_check_Central_South_list_df <- df_check_Central_South_list %>%
  cleaningtools::create_xlsx_cleaning_log(output_path = paste0("outputs/cleaning_logs/Central_South/logical_outlier_checks_Central_South.xlsx"),
                                          change_type_col = "change_type",
                                          cleaning_log_name = "cleaning_log",
                                          kobo_survey = survey,
                                          kobo_choices = choices,
                                          sm_dropdown_type = "logical",
                                          use_dropdown = TRUE)

df_check_Central_South_list <- clean_log_no_notes(df_check_Central_South_list)
df_check_Central_South_list$cleaning_log <- plyr::rbind.fill(df_check_Central_South_list$cleaning_log, table_dft_scs)
df_check_Central_South_list$cleaning_log <- left_join(df_check_Central_South_list$cleaning_log, df_translation, by = "question")
df_check_Central_South_list$cleaning_log <- df_check_Central_South_list$cleaning_log %>%
  mutate(explanation = " ") %>%
  relocate("issue",explanation,"old_value","new_value","change_type", .after = "label::arabic") %>% 
  mutate(unique_id = paste(uuid,question, sep = "_")) 



#Saving the cleaning logs for each organisation in Central South

org_list <- unique(df_check_Central_South_list$cleaning_log$organisation)
print(org_list)
for(org in org_list) {
  data_check_2 <- df_check_Central_South_list
  
  data_check_2$cleaning_log <- data_check_2$cleaning_log %>%
    filter(organisation == org)
  
  if (nrow(data_check_2$cleaning_log) > 0) {
    output_dir <- "outputs/cleaning_logs/Central_South/"
    if (!dir.exists(output_dir)) {
      dir.create(output_dir, recursive = TRUE)
    }
    
    create_combined_log(data_check_2) %>%
      create_xlsx_cleaning_log(output_path = paste0(output_dir, "Central_South_data_checks_", org, ".xlsx"),
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
south_central_outliers <- df_check_Central_South_list$cleaning_log
all_outliers <- rbind(northwest_outliers,northeast_outliers,south_central_outliers)
all_outliers_notes <- rbind(northeast_log_notes, northwest_log_notes, Central_South_log_notes)

write.xlsx(all_outliers, file = "outputs/cleaning_logs/outliers_log_checked_2025_04_15.xlsx")
write.xlsx(all_outliers_notes, file = "outputs/cleaning_logs/all_outliers_checked_with_notes_2025_04_15.xlsx")

#'---------------------------------------------------------------------------------------
# 5. Feedback integration ----
#'---------------------------------------------------------------------------------------
# read in the cleaned data
df_raw <- converted_data$Raw_data

# read in the cleaning logs

others_filled_logs <- read_excel("outputs/filled_cleaning_logs/others_filled_logs_2025_03_16.xlsx")
outliers_filled_log <- read_excel("outputs/filled_cleaning_logs/outliers_filled_logs_2025_03_19_updated.xlsx")

filled_logs <- plyr::rbind.fill(others_filled_logs, outliers_filled_log)

# reviewing the cleaning logs

cleaningtools::review_cleaning_log( raw_dataset = df_raw,
                                    raw_data_uuid_column = "X_uuid",
                                    cleaning_log = filled_logs,
                                    cleaning_log_change_type_column = "change_type",
                                    change_response_value = "change_response",
                                    cleaning_log_question_column = "question",
                                    cleaning_log_uuid_column = "uuid",
                                    cleaning_log_new_value_column = "new_value")



# function for actually doing data cleaning
df_clean <- create_clean_data(raw_dataset = df_raw, 
                              raw_data_uuid_column = 'X_uuid', 
                              cl = filled_logs,
                              cleaning_log_change_type_column = "change_type",
                              change_response_value = "change_response", 
                              NA_response_value = "blank_response", 
                              no_change_value = "no_action", 
                              remove_survey_value = "remove_survey",
                              cleaning_log_question_column =  "question",
                              cleaning_log_uuid_column = 'uuid',
                              cleaning_log_new_value_column = "new_value")


# redoing all the calculations in the data: read through the UDF:
calculations_data <- read_excel("functions/calculations_function.xlsx")

df_clean <- apply_calculations(df_clean)

#'---------------------------------------------------------------------------------------
# 6. Currency conversion after data cleaning----
#'---------------------------------------------------------------------------------------

converted_cleaned_data <- currency_conversion(df_clean)
cleaned_df_currency <- converted_cleaned_data$Data_NES_SRP_NWS_TRY


#'---------------------------------------------------------------------------------------
# 7. Counting of prices reported----
#'---------------------------------------------------------------------------------------

variables_to_aggregate <- names(fromJSON("./functions/external_to_internal_mapping.json"))
variables_to_impute <- variables_to_aggregate[!grepl("^price_smeb_", variables_to_aggregate)]

# Remove unwanted variables and add new ones
variables_to_impute <- setdiff(variables_to_impute, c("lpg_litre_price_item", "water_liter_price_item"))
variables_to_impute <- union(variables_to_impute, c("lpg_subsidised_price_item", "lpg_price_item", "water_truck_liter_price_unit_item"))

# Count price reports in df_clean while keeping all columns
dft_clean <- df_clean %>%
  mutate(price_reported_counts = rowSums(!is.na(select(., all_of(variables_to_impute))))) %>%
  group_by(admin4_label) %>%
  mutate(price_reported_counts_community_level = sum(price_reported_counts, na.rm = TRUE)) %>%
  ungroup()

# Count price reports in cleaned_converted_data while keeping all columns
cleaned_dft_currency <- cleaned_df_currency %>%
  mutate(price_reported_counts = rowSums(!is.na(select(., all_of(variables_to_impute))))) %>%
  group_by(admin4_label) %>%
  mutate(price_reported_counts_community_level = sum(price_reported_counts, na.rm = TRUE)) %>%
  ungroup()

#'---------------------------------------------------------------------------------------
# 8. Preparations of the deletion log ---
#'---------------------------------------------------------------------------------------
deletion_log <- dft_clean %>%
  filter(price_reported_counts == 0 | price_reported_counts_community_level == 1
         | X_uuid == "9f9a5ee7-3212-4e9a-8bbc-6d5ba93e7a2f") %>%
  select(uuid = X_uuid, deviceid, admin4_label, Enumerator_id = "enumerator") %>%
  mutate(Issue = "Low price reports",
         `Type of Issue` = "We did not collect prices for this shop",
         Feedback = NA_character_)

# Remove rows with no price reports and communities with only one price report
dft_clean <- dft_clean %>%
  filter(price_reported_counts > 0 ) %>% 
  filter(price_reported_counts_community_level > 1) %>% 
  filter(X_uuid != "9f9a5ee7-3212-4e9a-8bbc-6d5ba93e7a2f")

cleaned_dft_currency <- cleaned_dft_currency %>%
  filter(price_reported_counts > 0 )%>% 
  filter(price_reported_counts_community_level > 1) %>% 
  filter(X_uuid != "9f9a5ee7-3212-4e9a-8bbc-6d5ba93e7a2f")

#'---------------------------------------------------------------------------------------
# 9. Final data formating  ---
#'---------------------------------------------------------------------------------------

# adding admin names
geocodes_df <- load_geocode_data("inputs/geo_codes/SYR_REACH_admin4_February2024.xlsx",
                                 "inputs/geo_codes/1_Geopoints_Pcode_check.xlsx")



dft_clean <- clean_and_join_geocodes(dft_clean, geocodes_df)
cleaned_dft_currency <- clean_and_join_geocodes(cleaned_dft_currency, geocodes_df)

cleaned_jmmi_data <- list(
  "raw_data" = df_raw,
  "cleaned_data" = dft_clean,
  "cleaned_converted_data" = cleaned_dft_currency,
  "nes" =cleaned_dft_currency %>% filter(region == "Northeast"),
  "nws" = cleaned_dft_currency %>% filter(region == "Northwest"),
  "deletion_log" = deletion_log,
  "cleaning_log_others" = others_filled_logs,
  "cleaning_log_outliers" = outliers_filled_log
)

# Save cleaned data to Excel
output_path <- paste0("outputs/cleaned_data/", format(Sys.time(), "%Y_%m_%d_%H%M"), "_JMMI_cleaned_data.xlsx")
write.xlsx(cleaned_jmmi_data, file = output_path)

