library(readxl)
library(openxlsx)
library(dplyr)
library(tidyverse)

# Define folder path
folder_path <- "outputs/filled_cleaning_logs/Received_from_field//"

# Function to read and bind Excel files matching a pattern
read_and_bind_files <- function(pattern) {
  files <- list.files(folder_path, pattern = pattern, full.names = TRUE)
  data_list <- lapply(setNames(files, basename(files)), read_excel)
  do.call(plyr::rbind.fill, data_list)
}

# Load and merge reviewed logs
draft_logs_filled <- read_and_bind_files("_reviewed\\.xlsx$")

# Load and merge reviewed logs for Haseka
H_draft_logs_filled <- read_and_bind_files("_reviewed_H\\.xlsx$") %>%
  filter(area == "H")

# Combine all logs
tab_logs_filled <- plyr::rbind.fill(draft_logs_filled, H_draft_logs_filled)

# Save combined logs
write.xlsx(tab_logs_filled, file = "outputs/filled_cleaning_logs/Received_from_field/tab_logs_filled.xlsx")

# Update logs with new values
tab_logs_filled <- tab_logs_filled %>%
  mutate(
    new_value = case_when(
      new_value == "Confirm" ~ old_value,
      is.na(new_value) ~ old_value,
      change_type == "remove_survey" ~ NA_character_,
      TRUE ~ new_value
    ),
    change_type = case_when(
      new_value == old_value ~ "no_action",
      new_value != old_value ~ "change_response",
      TRUE ~ change_type
    ),
    unique_id = paste0(uuid, "_", question)
  ) %>%
  select(-c(area, checked))  # Remove unnecessary columns

# Check for duplicated unique IDs
duplicates_df <- tab_logs_filled %>%
  group_by(unique_id) %>%
  summarise(n = n()) %>%
  filter(n > 1)
print(nrow(duplicates_df))

# Save updated logs
write.xlsx(tab_logs_filled, file = "outputs/filled_cleaning_logs/Received_from_field/tab_logs_filled_step2.xlsx")

# Process outlier logs
log_all <- read_excel("outputs/filled_cleaning_logs/Received_from_field/outliers_log_checked_2025_03_16.xlsx") %>%
  mutate(
    checked = case_when(unique_id %in% tab_logs_filled$unique_id ~ "yes", TRUE ~ "no")
  ) %>%
  filter(checked == "no") %>%
  mutate(
    new_value = case_when(is.na(new_value) ~ old_value, TRUE ~ new_value),
    change_type = case_when(new_value == old_value ~ "no_action", TRUE ~ change_type),
    explanation = "confirmed from the notes"
  )

# Merge all logs
all_filled_logs <- plyr::rbind.fill(tab_logs_filled, log_all)

# Save merged logs
write.xlsx(all_filled_logs, file = "outputs/filled_cleaning_logs/outliers_filled_logs.xlsx")

# Load and merge "other" responses
tab_other_filled <- read_and_bind_files("_other_responses\\.xlsx$") %>%
  mutate(
    issue = "recode other",
    unique_id = paste0(uuid, "_", question)
  ) %>%
  select(-c("invalid_other", "existing_other", "checked")) %>%
  rename(new_value = true_other)

# Check for duplicates
other_duplicates_df <- tab_other_filled %>%
  group_by(unique_id) %>%
  summarise(n = n()) %>%
  filter(n > 1)
print(nrow(other_duplicates_df))

# Save "other" filled logs
write.xlsx(tab_other_filled, file = "outputs/filled_cleaning_logs/Received_from_field/other_data_filled_logs.xlsx")

# Clean "other" responses
tab_other_filled_check <- tab_other_filled %>%
  mutate(
    new_value = case_when(
      old_value == "." ~ NA_character_,
      new_value %in% c("Yes", "Yes.", "Yes.Ktif1") ~ NA_character_,
      str_detect(new_value, "reason") ~ NA_character_,
      str_detect(new_value, "comment") ~ NA_character_,
      new_value %in% c("No", "None", "No change in price", "There isn't any", "Joseph Water 3",
                       "Syria Phone", "Yes, good.", "Ammar", "Yes, Ammar.", "There isn't any",
                       "52.5", "10$") ~ NA_character_,
      str_detect(new_value, "^\\d+$") ~ NA_character_,  # Remove numeric values
      TRUE ~ new_value),
    change_type = case_when(is.na(new_value) ~ "blank_response",
                            TRUE ~ "change_response")
  )

# Save final cleaned "other" responses
write.xlsx(tab_other_filled_check, file = "outputs/filled_cleaning_logs/Received_from_field/other_data_filled_logs.xlsx")
write.xlsx(tab_other_filled_check, file = "outputs/filled_cleaning_logs/others_filled_logs.xlsx")
