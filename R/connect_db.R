#' Connect to the ADEM PostgreSQL Database
#'
#' Establishes a connection to the PostgreSQL database using credentials
#' and host information stored in environment variables. This function is used
#' internally by other functions that need to interact with the ADEM database.
#'
#' Environment variables expected:
#' - PG_DB: database name
#' - PG_HOST: database host
#' - PG_USER: database username
#' - PG_PASSWORD: database password
#'
#' @return A DBI connection object (class `"PqConnection"`); or NULL, in case any error occurs and the DB connection could not be established.
#'
#' @examples
#' \dontrun{
#' con <- connect_db()
#' DBI::dbListTables(con)
#' DBI::dbDisconnect(con)
#' }
connect_db <- function() {

  db_var_name = "PG_DB"
  host_var_name = "PG_HOST"
  user_var_name = "PG_USER"
  password_var_name = "PG_PASSWORD"

  if ((env_var_set_not_empty(db_var_name)) &
      (env_var_set_not_empty(host_var_name)) &
      (env_var_set_not_empty(user_var_name)) &
      (env_var_set_not_empty(password_var_name)))
    tryCatch(
      {
        con <- DBI::dbConnect(
          RPostgres::Postgres(),
          dbname = Sys.getenv(db_var_name),
          host = Sys.getenv(host_var_name),
          user = Sys.getenv(user_var_name),
          password = Sys.getenv(password_var_name),
          port = 5432
        )
        message("Database connection successful!")
        return(con)
      },
      error = function(e) {
        message("Error: Database connection failed: ", e$message)
        return(NULL)
      })
}


#' Checks if an environment variable is set (in .Renviron) but is not an empty string (if set, it will always be a string).
#'
#' @param env_var_name Name of environment variable to be checked
#'
#' @returns TRUE, if the checks are successful; FALSE, in case any of the criteria are not matched.
#'
#' @examples
#' \dontrun{
#' env_var_set_not_empty("PG_DB")
#' env_var_set_not_empty("any-variable-name")
#' }
env_var_set_not_empty <- function (env_var_name) {
  env_var <- Sys.getenv(env_var_name, unset = NA)

  if (is.na(env_var)) {
    warning(paste("Error: Environment variable '", env_var_name, "' is not set."))
    return(FALSE)
  }
  if (nchar(env_var) == 0) {
    warning(paste("Error: Environment variable '", env_var_name, "' is an empty string."))
    return(FALSE)
  }
  return(TRUE)
}

