
# return total for last available month plus variation vs. selected reference year/month (for comparison)
oe_current_total <- function (db_conn) {

}

# return total new for last available month plus variation vs. selected reference year/month (for comparison)
oe_new_in_month <- function (db_conn) {

}

# --------------------------------------------------------------------

# return monthly value for the past 48 months (also for bar chart at the bottom of pillar)
oe_per_month <- function (db_conn) {

}

# return values for last available month, or given year/month
oe_per_type <- function (year = lubridate::year(Sys.Date()), month = lubridate::month(Sys.Date()), db_conn) {

}

# return values for top 10 of last available month, or given year/month
oe_per_metier <- function (year = lubridate::year(Sys.Date()), month = lubridate::month(Sys.Date()), db_conn) {

}

