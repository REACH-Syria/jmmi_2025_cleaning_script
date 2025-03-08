
#'---------------------------------------------------------------------------------------
# creating directories ----
#'---------------------------------------------------------------------------------------

create_directories <- function(base_dir = "outputs", dirs = NULL) {
  if (is.null(dirs)) {
    dirs <- c(
      "cleaning_logs",
      "enumerator_checks",
      "filled_cleaning_logs",
      "raw_data_converted"
    )
  }
  
  lapply(file.path(base_dir, dirs), dir.create, recursive = TRUE, showWarnings = FALSE)
}


#'---------------------------------------------------------------------------------------
# reading in data and the tool ----
#'---------------------------------------------------------------------------------------
load_data_and_tool <- function(filename.dataset, koboToolPath) {
  

  raw <- read_excel(filename.dataset, col_types = "text") %>%
    mutate_at(c("start", "end", "date"), ~ as.character(convertToDateTime(as.numeric(.)))) %>%
    mutate_at(c("_submission_time"), ~ as.character(as.Date(as.numeric(.), origin = "1899-12-30"))) %>%
    dplyr::rename(X_uuid = '_uuid')
  

  tool.survey <- read_xlsx(koboToolPath,
                           guess_max = 50000,
                           na = c("NA", "", " ", "#N/A", "N/A", "n/a"),
                           sheet = "survey") %>%
    filter(!is.na(name)) %>%
    mutate(
      q.type = as.character(lapply(type, function(x) str_split(x, " ")[[1]][1])),
      list_name = as.character(lapply(type, function(x) str_split(x, " ")[[1]][2])),
      list_name = ifelse(str_starts(type, "select_"), list_name, NA)
    )
  

  tool.choices <- read_xlsx(koboToolPath,
                            guess_max = 50000,
                            na = c("NA", "", " ", "#N/A", "N/A", "n/a"),
                            sheet = "choices")
  
  
  return(list(raw = raw, tool_survey = tool.survey, tool_choices = tool.choices))
}

#'---------------------------------------------------------------------------------------
# Other files ----
#'---------------------------------------------------------------------------------------


X2025_03_04_other_responses <- read_excel("outputs/cleaning_logs/2025-03-04_other_responses.xlsx") %>% 
  mutate(unique_id = paste(uuid,name, sep = "_"))

X2025_03_05_1029_other_log_translated <- read_excel("outputs/cleaning_logs/2025_03_05_1029_other_log_translated.xlsx") %>% 
  mutate(unique_id = paste(uuid,question, sep = "_"))

X2025_03_06_1008_other_responses <- read_excel("outputs/cleaning_logs/2025_03_06_1008_other_responses.xlsx")

others_bind <- plyr::rbind.fill(X2025_03_04_other_responses,X2025_03_05_1029_other_log_translated,
                                X2025_03_06_1008_other_responses)

#'---------------------------------------------------------------------------------------
# currency conversion ----
#'---------------------------------------------------------------------------------------

#'*EXCHANGE RATES USING MEDIANS OF THE EXCAHNGE RATE REPORTED FOR NORTHEAST AND NORTHWEST SYRIA*
# calculate medians of exchange rates: Turkish_lira-Syrian_pound & US_dollar-Syrian_pound for each region
# Any calculate ending with "_price_item" is the price in standard unit KG/L.
# getting the prices that standard.
# there was a mistake on the soap_bar_price_item calculation therefore we need
# to delete it and calculate it again
# soap_price_item: This should give the price based on the unit selected
# soap_bar_price_item: This should give the price in KG
#df$soap_bar_price_item <- NULL
currency_conversion <- function(df){
  
  
  items.list.data <- df %>% dplyr::select(ends_with("_price_item"),
                                          water_truck_liter_price_unit_item,
                                          exchange_rate_buy_usd_syp,
                                          exchange_rate_sell_usd_syp,
                                          exchange_rate_buy_usd_try,
                                          exchange_rate_sell_usd_try,
                                          internet_data_unit_item)
  
  
  items.list <- names(items.list.data)
  
  df <- df %>%
    mutate_at(items.list, as.numeric) %>% 
    mutate(region = country_area_label)
  
  
  
  # we do not want to convert the exchange rates
  items.list <- items.list[!str_detect(items.list,"exchange_rate_") &
                             !str_detect(items.list,"internet_data_modem_unit_item") &
                             !str_detect(items.list,"internet_data_unit_item")]
  
  
  
  
  df_currency <- df %>%
    dplyr::mutate(
      exchange_rate_medians_sell = case_when(
        
        region == "Northeast" & shop_currency == "US_dollar" ~ median(exchange_rate_sell_usd_syp, na.rm = TRUE),
        region == "Northeast" & shop_currency == "Syrian_pound" ~ 1,
        region == "Northwest" & shop_currency == "US_dollar" ~ median(exchange_rate_sell_usd_try, na.rm = TRUE),
        region == "Northwest" & shop_currency == "Syrian_pound" ~ (1/440),
        region == "Northwest" & shop_currency == "Turkish_lira" ~ 1,
        TRUE ~ NA_real_
      )
    )
  
  unique(df_currency$exchange_rate_medians_sell)
  
  Currency_change <- function(df, items.list) {
    for (col.price in items.list) {
      df <- df %>%
        dplyr::mutate(!!sym(col.price) := (!!sym(col.price) * exchange_rate_medians_sell))
    }
    return(df)
  }
  
  
  currency.converted <- Currency_change(df_currency, items.list)
  
  
  
  nes_converted_Syrian_pound <- currency.converted %>% filter(region == "Northeast")
  nws_converted_Turkish_lira <- currency.converted %>% filter(region == "Northwest")
  
  ##Saving all the datasets
  market_monitoring_raw_data <-
    list(
      "Raw_data" = df,
      "Data_NES_SRP_NWS_TRY" = currency.converted,
      "Converted_NES_SRP" = nes_converted_Syrian_pound,
      "Converted_NWS_TRY" = nws_converted_Turkish_lira
    )
  return(market_monitoring_raw_data)
}

#'---------------------------------------------------------------------------------------
# Outlier  function ----
#'---------------------------------------------------------------------------------------

#creating the outlier function to use in the logical checks
check_outlier <- function(df,x,var_name,aggregation_var=NULL,coef = 1.5) {
  
  if(is.null(aggregation_var)){
    return(is_outlier(x,coef = coef))
  } else {
    df %>% group_by(!!sym(aggregation_var)) %>% mutate(
      is_outlier = is_outlier(!!sym(var_name),coef = coef)
    ) %>% pull(is_outlier)
    
  }
}

#'---------------------------------------------------------------------------------------
# Outlier fuel, water, internet function ----
#'---------------------------------------------------------------------------------------
outliers_fuel_water <- function(dft, df) {
  
  
  df_counts_outliers <- dft %>%
    select(
      X_uuid, region, organisation, deviceid, enumerator, admin4_label, shop_currency,
      diesel_blackmarket_price_item, petrol_blackmarket_price_item, gas_blackmarket_price_item, 
      kerosene_blackmarket_price_item, diesel_local_subsidised_price_item, 
      diesel_local_price_item, petrol_local_subsidised_price_item, petrol_local_price_item, 
      diesel_imported_price_item, petrol_imported_price_item, lpg_subsidised_price_item, 
      kerosene_subsidised_price_item, lpg_price_item, kerosene_price_item, 
      water_truck_liter_price_unit_item
    )
  
  # Northeast Outlier Check
  df_nes <- df_counts_outliers %>% filter(region == "Northeast")
  outliers_nes <- cleaningtools::check_outliers(dataset = df_nes, uuid_column = "X_uuid", sm_separator = "/")
  
  outliers_nes <- add_info_to_cleaning_log(
    list_of_log = outliers_nes,
    dataset = "checked_dataset",
    cleaning_log = "potential_outliers",
    dataset_uuid_column = "X_uuid",
    cleaning_log_uuid_column = "uuid",
    information_to_add = c("region", "organisation", "deviceid", "enumerator", 
                           "admin4_label", "shop_currency")
  )
  
  outliers_log_nes <- outliers_nes$cleaning_log
  
  # Northwest Outlier Check
  df_nws <- df_counts_outliers %>% filter(region == "Northwest")
  outliers_nws <- cleaningtools::check_outliers(dataset = df_nws, uuid_column = "X_uuid", sm_separator = "/")
  
  outliers_nws <- add_info_to_cleaning_log(
    list_of_log = outliers_nws,
    dataset = "checked_dataset",
    cleaning_log = "potential_outliers",
    dataset_uuid_column = "X_uuid",
    cleaning_log_uuid_column = "uuid",
    information_to_add = c("region", "organisation", "deviceid", "enumerator", 
                           "admin4_label", "shop_currency")
  )
  
  outliers_log_nws <- outliers_nws$cleaning_log
  
  # Bind Outliers from Both Regions
  potential_outliers <- rbind(outliers_log_nes, outliers_log_nws) %>%
    filter(question %nin% c("index", "_submission_time", "_id")) %>%
    filter(!is.na(old_value))
  
  # Check Outliers in Original Unconverted Data
  # df <- df %>% mutate_at(items.list, as.numeric)
  
  for (i in 1:nrow(potential_outliers)) {
    id <- as.character(potential_outliers[i, "uuid"])
    col <- as.character(potential_outliers[i, "question"])
    new.value <- df[df$X_uuid == id, col]
    
    potential_outliers[potential_outliers$uuid == id & potential_outliers$question == col, "value"] <- new.value
  }
  
  potential_outliers <- potential_outliers %>% filter(value != 0)
  
  # Convert and Finalize Outlier Table
  potential_outliers$old_value <- NULL  # Remove converted value
  
  potential_outliers_final <- potential_outliers %>%
    rename("old_value" = "value")
  
  return(potential_outliers_final)
}

# Generate Outlier Tables

# table_dft <- outliers_fuel_water(df_currency, raw_df)
# table_dft_west <- table_dft %>% filter(region == "Northwest")
# table_dft_east <- table_dft %>% filter(region == "Northeast")

#'---------------------------------------------------------------------------------------
# questions preparation for the data merge ----
#'---------------------------------------------------------------------------------------

df_translation <- survey %>%
  dplyr::select(`label::arabic`,name)%>%
  na.omit() %>%
  dplyr::rename("question" = name)




#'---------------------------------------------------------------------------------------
# list formating functions ----
#'---------------------------------------------------------------------------------------

# checking if the outliers have notes

clean_log_with_notes <- function(df_list) {
  df_list$cleaning_log <- df_list$cleaning_log %>%
    mutate(
      old_value = case_when(old_value == "NA" ~ NA_character_, TRUE ~ old_value)
    ) %>%
    group_by(check_binding) %>%
    mutate(
      Note = case_when(
        any(str_detect(old_value, "[:alpha:]")) ~ "has_note",
        TRUE ~ "no_note"
      )
    ) %>%
    ungroup()
  
  return(df_list)
}

# to share with partners
clean_log_no_notes <- function(df_list){
  df_list$cleaning_log <- df_list$cleaning_log %>%
    dplyr::filter(Note == "no_note") %>%
    filter(str_ends(question, "price_unit_item")|
             question %in% c("country_area_label",
                             "shop_currency",
                             "admin4_label",
                             "internet_data_modem_std_unit",
                             "internet_data_std_unit",
                             "internet_data_unit_item",
                             "internet_data_std_unit",
                             "exchange_rate_buy_usd_syp",
                             "exchange_rate_buy_try_syp",
                             "exchange_rate_sell_try_syp",
                             "exchange_rate_buy_usd_try",
                             "exchange_rate_sell_usd_try"
             ))
  return(df_list)
}

# filtering out the shared cleaning logs
# read in the previous files for yesterday
log_reach <- read_excel("outputs/cleaning_logs/northeast/2025_03_05_northeast_data_checks_REACH.xlsx", 
                        sheet = "cleaning_log")
log_soli <- read_excel("outputs/cleaning_logs/northeast/2025_03_05_northeast_data_checks_Solidarites_International.xlsx", 
                        sheet = "cleaning_log")

X2025_03_06_log_reach <- read_excel("outputs/cleaning_logs/northeast/2025_03_06_northeast_data_checks_REACH.xlsx",
                                                      sheet = "cleaning_log")

X2025_03_06_log_soli <- read_excel("outputs/cleaning_logs/northeast/2025_03_06_northeast_data_checks_Solidarites_International.xlsx",
                                    sheet = "cleaning_log")

log_nes <- plyr::rbind.fill(log_reach,log_soli, X2025_03_06_log_reach, X2025_03_06_log_soli)

log_nes <- log_nes %>% 
  mutate(unique_id = paste(uuid,question, sep = "_"))

log_reach <- read_excel("outputs/cleaning_logs/northwest/2025_03_05_northwest_data_checks_REACH.xlsx", 
                      sheet = "cleaning_log") 
 
X2025_03_06_northwest_ATAA <- read_excel("outputs/cleaning_logs/northwest/2025_03_06_northwest_data_checks_ATAA.xlsx",
                                         sheet = "cleaning_log") 

X2025_03_06_northwest_reach <- read_excel("outputs/cleaning_logs/northwest/2025_03_06_northwest_data_checks_REACH.xlsx",
                                         sheet = "cleaning_log") 

log_nws <- plyr::rbind.fill(log_reach,X2025_03_06_northwest_ATAA,X2025_03_06_northwest_reach)%>%
  mutate(unique_id = paste(uuid,question, sep = "_"))

