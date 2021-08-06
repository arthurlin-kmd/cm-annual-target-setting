library(tidyverse)
library(keyring)
library(dbplyr)
library(lubridate)
library(DBI)
library(kmdr)
con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "BIDW", 
                      Database = "data_warehouse", UID = keyring::key_get("email_address"), 
                      PWD = keyring::key_get("kmd-password"), Trusted_Connection = "TRUE", 
                      Port = 1433)


# params ------------------------------------------------------------------

tbl_database <- set_source_tables(con)

date_start <- as.Date("2020-08-01")

date_end <- date_start + months(12) - days(1)


# data extraction ---------------------------------------------------------


get_mbr_spend_profile <- function(date_start = date_start, date_end = date_end) {
    
    # get spend profile for current year
    sales_current_year_tbl <- kmdr::base_txn_query(con, period_start = date_start, period_end = date_end) %>%
        filter(customer_type == "Summit Club") %>% 
        
        group_by(sales_country) %>% 
        summarise(
            mbr_revenue = sum(sale_amount_excl_gst, na.rm =  TRUE),
            mbr_txn     = n_distinct(sale_transaction)
        ) %>% 
        collect() %>% 
        ungroup() %>% 
        
        mutate(mbr_revenue = scales::dollar(mbr_revenue)) 
    
    return(sales_current_year_tbl)
    
    # get spend profile for the previous year
    date_start_LY = date_start - months(12)
    
    date_end_LY = date_start_LY - days(1)
    
    
    sales_last_year_tbl <- kmdr::base_txn_query(con, period_start = date_start_LY, period_end = date_end_LY) %>%
        filter(customer_type == "Summit Club") %>% 
        
        group_by(sales_country) %>% 
        summarise(
            mbr_revenue = sum(sale_amount_excl_gst, na.rm =  TRUE),
            mbr_txn     = n_distinct(sale_transaction)
        ) %>% 
        collect() %>% 
        ungroup() %>% 
        
        mutate(mbr_revenue = scales::dollar(mbr_revenue)) 
    
    return(sales_last_year_tbl)
    
    
}

get_mbr_spend_profile(date_start, date_end)






