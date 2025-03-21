
# Load necessary libraries
library(tidyverse)
library(readxl)
library(dplyr)
library(openxlsx)

koboToolPath = "inputs/TOOL_JMMI_MARCH.xlsx"
questions <- read_xlsx(koboToolPath,
                       guess_max = 50000,
                       na = c("NA", "", " ", "#N/A", "N/A", "n/a"),
                       sheet = "survey") %>% filter(!is.na(name)) %>%
  mutate(q.type=as.character(lapply(type, function(x) str_split(x, " ")[[1]][1])),
         list_name=as.character(lapply(type, function(x) str_split(x, " ")[[1]][2])),
         list_name=ifelse(str_starts(type, "select_"), list_name, NA))

# Function to convert calculation to R code
convert_to_r <- function(calculation) {
  calculation_r <- gsub("\\$\\{([a-zA-Z0-9_]+)\\}", "\\1", calculation) # Remove ${} notation
  calculation_r <- gsub("if\\(selected\\(([^,]+),'Yes'\\),", "if(\\1 == 'Yes',", calculation_r) # Convert selected() to == 'Yes'
  calculation_r <- gsub(" div ", " / ", calculation_r) # Convert div to /
  calculation_r <- gsub("if\\(", "ifelse(", calculation_r) # Convert if() to ifelse()
  return(calculation_r)
}


# Apply the function to the calculations column and store the R code
# get the calculations
questions_calc <- questions %>%
  filter(type == "calculate") %>%
  filter(name %nin% c("organisation_name_label", "SYR_country_area_label_tx",
                      "SYR_shop_currency_label_tx"))

# apply the function to the calculations
calculations_data <- questions_calc %>%
  dplyr::mutate(calculation_r = sapply(calculation, convert_to_r))

calculations_data <- calculations_data%>%
mutate(calculation_r = case_when(name == "Calc_capacity_water_truck" ~ "ifelse(SYR_water_truck_unit_nt == 'Litres', SYR_water_truck_capacity_nt, ifelse(SYR_water_truck_unit_nt == 'Cubic_meters', SYR_water_truck_capacity_nt * 1000, ifelse(SYR_water_truck_unit_nt == 'Barrels', SYR_water_truck_capacity_nt * 200, NA)))",
                                 name == "internet_data_price_item" ~ "ifelse(internet_data_std_unit == 'GB',(internet_data_price_unit_item / internet_data_unit_item),((internet_data_price_unit_item * 1000) / internet_data_unit_item))",
                                 name == "diaper_price_item" ~ "diaper_price_unit_item * 1",
                                 TRUE ~ calculation_r))

write.xlsx(calculations_data, file = "functions/calculations_function.xlsx")


#save the file and edit the following calculations
# ifelse(SYR_water_truck_unit_nt == 'Litres', SYR_water_truck_capacity_nt, ifelse(SYR_water_truck_unit_nt == 'Cubic_meters', SYR_water_truck_capacity_nt * 1000, ifelse(SYR_water_truck_unit_nt == 'Barrels', SYR_water_truck_capacity_nt * 200, NA)))"

# ifelse(internet_data_std_unit == 'GB',(internet_data_price_unit_item / internet_data_unit_item),((internet_data_price_unit_item * 1000) / internet_data_unit_item))


# Define a user-defined function to apply transformations to a dataset
apply_calculations <- function(input_data) {
  # Create a copy of the input data to avoid modifying the original dataset
  output_data <- input_data

  # Apply each calculation to the dataset
  for (i in 1:nrow(calculations_data)) {
    name <- calculations_data$name[i]
    calculation_r <- calculations_data$calculation_r[i]

    # Generate the expression and evaluate it
    expr <- parse(text = paste0(name, " <- ", calculation_r))
    output_data <- within(output_data, eval(expr))
  }

  return(output_data)
}




# transformed_data <- apply_calculations(dft)
#
# # Save the transformed data to a new Excel file
# # output_file_path <- "path_to_your_output_data.xlsx"
# # write.xlsx(transformed_data, file = output_file_path)
#
# # Print a message to indicate successful completion
# # cat("Transformed data has been saved to", output_file_path)
#
# write.xlsx(df, file.path(cleaning_log_dir, "df_with_pcodes.xlsx"))

