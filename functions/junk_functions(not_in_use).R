
price_items <- c(
  "diesel_blackmarket_price_item", "petrol_blackmarket_price_item", "gas_blackmarket_price_item",
  "kerosene_blackmarket_price_item", "diesel_local_subsidised_price_item", "petrol_local_subsidised_price_item",
  "diesel_imported_price_item", "petrol_imported_price_item", "diesel_local_price_item",
  "petrol_local_price_item", "lpg_subsidised_price_item", "lpg_price_item",
  "kerosene_subsidised_price_item", "kerosene_price_item"
)

cleaned_data_oct <- read_excel("input/cleaned_data_oct.xlsx")



aggregate_vars_by_group <- function(dft, variables_to_aggregate, group_by) {
  dft <- dft %>%
    group_by_at(vars(one_of(group_by))) %>%
    summarise_at(vars(variables_to_aggregate), median, na.rm = TRUE) %>%
    ungroup()
  return(dft)
}
fuel_data <- df_currency %>%
  aggregate_vars_by_group(price_items, group_by = "region")

write.xlsx(fuel_data, "outputs/fuel_data_oct.xlsx")

