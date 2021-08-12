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

date_start <- as.Date("2018-08-01")

date_end <- date_start + months(36) - days(1)


# data extraction ---------------------------------------------------------


get_mbr_spend_profile <- function(date_start = date_start, date_end = date_end) {
    
    # get spend profile for current year
    fy_country_sales_tbl <- kmdr::base_txn_query(con, period_start = date_start, period_end = date_end) %>%
        filter(customer_type == "Summit Club") %>% 
        
        group_by(fin_year, sales_country) %>% 
        summarise(
            mbr_revenue = sum(sale_amount_excl_gst, na.rm =  TRUE),
            mbr_txn     = n_distinct(sale_transaction)
        ) %>% 
        collect() %>% 
        ungroup() %>% 
        
        mutate(mbr_revenue = scales::dollar(mbr_revenue)) %>%  
        arrange(fy_country_sales_tbl)
    
    return(fy_country_sales_tbl)
}

fy_country_sales_tbl <- get_mbr_spend_profile(date_start, date_end)


build_date_range <- function(from, to, by){
    
    period_start <- as.Date(from)
    
    period_end <- as.Date(to)
    
    if (!by %in% c("day", "month", "week", "year")) {
        stop("`by` must be 'day', 'week' or 'month'", call. = FALSE)
    }
    
    date_range <- dplyr::tibble(period_start= seq.Date(from = period_start, to = period_end, by =by),
                                
                    period_end = dplyr::case_when(
                        by== "month" ~ lubridate::ceiling_date(period_start, "month") - lubridate::days(1),
                        by == "week"~ period_start + lubridate::days(6),
                        TRUE ~ period_start))

    return(date_range)
    
}

build_date_range('2019-08-01','2021-07-31', 'month')





