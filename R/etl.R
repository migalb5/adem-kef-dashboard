


#df_de_nonresid <- rio::import("https://data.public.lu/fr/datasets/r/76660a32-329e-4ae5-81af-1f3f6b42718d")



# -------------------------------------------------------------------------------------------------------------------

extract_transform_table <- function (fq_table_name) {
  message(paste("Extracting and transforming available source data for table: ", fq_table_name))
  df_trf <- switch (fq_table_name,
    "student_miguel.kef_de_metier" = {
      df <- rio::import("https://data.public.lu/fr/datasets/r/2114333b-e66e-4511-971f-90a60e089864")
      df <- df %>% select("Date", "Genre", "ROME_niveau1", "ROME_niveau2", "Personnes")
      colnames(df) <- c("date", "genre_id", "rome_n1", "rome_n2", "rome_n2_total")
      df <- df %>% group_by(date, genre_id, rome_n1, rome_n2) %>% summarise(rome_n2_total = sum(rome_n2_total))
    },
    "student_miguel.kef_de_profil" = {
      df <- rio::import("https://data.public.lu/fr/datasets/r/6074ffe2-31fb-4d02-98c0-288ed0b1d26e")
      df <- df %>% select("Date", "Genre", "Age", "Niveau_de_diplome", "Duree_d_inscription", "Personnes")
      colnames(df) <- c("date", "genre_id", "age_group_id", "educ_level_id", "reg_dur_low_id", "total")
      df
    },
    "student_miguel.kef_de_mesure" = {
      df <- rio::import("https://data.public.lu/fr/datasets/r/8cfcdd92-f26d-46e8-bcc3-e2bc9fb8b557")
      df <- df %>% select("Date", "Genre", "Age", "Mesure", "Personnes")
      colnames(df) <- c("date", "genre_id", "age_group_id", "measure_id", "total")
      df
    },
    "student_miguel.kef_de_age_dur" = {
      df <- rio::import("https://data.public.lu/fr/datasets/r/a41b3377-1071-4b17-ac78-47f596897b53")
      df <- df %>% select("Date", "Age", "Duree_inscription", "Personnes")
      colnames(df) <- c("date", "age_group_low_id", "reg_dur_id", "total")
      df
    },
    "student_miguel.kef_de_res" = {
      # de-indemnites.csv
      df <- rio::import("https://data.public.lu/fr/datasets/r/bf01e691-d3ca-4889-adaa-d2db86d5688b")
      df <- df %>% select("Date", "Genre", "Age", "Residence", "Chomage_complet", "Indemnite_professionnelle_d_attente")
      colnames(df) <- c("date", "genre_id", "age_group_id", "res_id", "cho_comp_total", "ipa_total")
      df
    },
    "student_miguel.kef_de_nat" = {
      df <- rio::import("https://data.public.lu/fr/datasets/r/2b3ca540-9131-4d95-bf1a-a21d9b75b225")
      df <- df %>% select("Date", "Nationalite", "Genre", "Personnes")
      colnames(df) <- c("date", "nat_id", "genre_id", "total")
      df
    },
    "student_miguel.kef_oe" = {
      df <- rio::import("https://data.public.lu/fr/datasets/r/82415dc2-520e-4e93-b07e-74f0c85039ec")
      df <- df %>% select("Date", "Nature_contrat", "ROME_metier", "Postes_declares", "Stock_postes_vacants")
      colnames(df) <- c("date", "contract_type_id", "metier_id", "declared_total", "stock_total")
      df
    }
  )
  browser()

  # date transformation common to all tables (being sourced)
  df_trf$date <- format(as.Date(df_trf$date, format = "%d-%m-%Y"), "%Y-%m-%d")
  message("Extraction/transformation complete.")
  browser()
  return(df_trf)
}

# -------------------------------------------------------------------------------------------------------------------

update_table <- function (db_conn, df_trf, fq_table_name) {
  if (DBI::dbIsValid(db_conn)) {
    message(paste("Loading new data into table: ", fq_table_name))
    query = glue::glue("SELECT MAX(date) FROM {`fq_table_name`}", .con = db_conn)
    df <- DBI::dbGetQuery(db_conn, query)
    parts <- strsplit(fq_table_name, "\\.")[[1]]
    schema_name <- parts[1]
    table_name <- parts[2]
    df_new <- df_trf %>% filter(date >= "2009-01-01")
    if (!is.na(df[[1]]))
      df_new <- df_new %>% filter(date > df[[1]])
    message(paste("Latest date found: ", df[[1]]))
    browser()
    rows_inserted <- RPostgres::dbAppendTable(db_conn, DBI::Id(schema = schema_name, table = table_name), df_new)
    message(paste("Loading completed. New records inserted: ", rows_inserted))
    browser()
  } else {
    message("DB connection failed/not valid!")
    return(-1)
  }
}
