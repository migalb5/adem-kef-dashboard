
library(rio)
library(lubridate)
library(DBI)
library(RPostgres)
library(dplyr)
library(glue)
library(stringr)
library(stringi)

# -----------------------------------------------------------------------------

df_de_profil <- rio::import("https://data.public.lu/fr/datasets/r/6074ffe2-31fb-4d02-98c0-288ed0b1d26e")

df_de_age_duree <- rio::import("https://data.public.lu/fr/datasets/r/a41b3377-1071-4b17-ac78-47f596897b53")

df_de_metier <- rio::import("https://data.public.lu/fr/datasets/r/2114333b-e66e-4511-971f-90a60e089864")

df_de_nationalite <- rio::import("https://data.public.lu/fr/datasets/r/2b3ca540-9131-4d95-bf1a-a21d9b75b225")

df_de_mesure <- rio::import("https://data.public.lu/fr/datasets/r/8cfcdd92-f26d-46e8-bcc3-e2bc9fb8b557")

df_de_indemnite <- rio::import("https://data.public.lu/fr/datasets/r/bf01e691-d3ca-4889-adaa-d2db86d5688b")
df_de_nonresid <- rio::import("https://data.public.lu/fr/datasets/r/76660a32-329e-4ae5-81af-1f3f6b42718d")

df_offre_detail <- rio::import("https://data.public.lu/fr/datasets/r/82415dc2-520e-4e93-b07e-74f0c85039ec")

# -----------------------------------------------------------------------------

db_conn <- connect_db()

DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_genre")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_age_group")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_age_group_low")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_reg_dur")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_reg_dur_low")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_nat")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_educ_level")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_res")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_mesure")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_metier")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_contract_type")
DBI::dbExecute(db_conn, "DELETE FROM student_miguel.kef_log")

# -----------------------------------------------------------------------------

#insert_base_values <- function (df_source, df_column_index, db_table_name, db_table_field_name) {}

df_genres <- df_de_profil %>%
  distinct(Genre) %>%
  mutate(
    Genre = stringr::str_replace_all(Genre, "\\s+", ""),  # remove all whitespace characters
    Genre = stringr::str_trim(Genre)                      # remove leading/trailing just in case
  ) %>%
  filter(Genre != "", !is.na(Genre))                      # remove empty values and NA values

for (i in 1:nrow(df_genres)) {
  value = df_genres$Genre[i]
  ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_genre (id) VALUES ({value})", .con = db_conn)
  DBI::dbExecute(db_conn, ins_stmt)
}

# -----------------------------------------------------------------------------

df_age_groups <- df_de_profil %>%
  distinct(Age) %>%
  mutate(
    Age = stringr::str_replace_all(Age, "\\s+", ""),  # remove all whitespace characters
    Age = stringr::str_trim(Age)                      # remove leading/trailing just in case
  ) %>%
  filter(Age != "", !is.na(Age))                      # remove empty values and NA values

for (i in 1:nrow(df_age_groups)) {
  value = df_age_groups$Age[i]
  ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_age_group (id) VALUES ({value})", .con = db_conn)
  DBI::dbExecute(db_conn, ins_stmt)
}

# -----------------------------------------------------------------------------

df_age_groups_low <- df_de_age_duree %>%
  distinct(Age) %>%
  mutate(
    Age = stringr::str_replace_all(Age, "\\s+", ""),  # remove all whitespace characters
    Age = stringr::str_trim(Age)                      # remove leading/trailing just in case
  ) %>%
  filter(Age != "", !is.na(Age))                      # remove empty values and NA values

for (i in 1:nrow(df_age_groups_low)) {
  value = df_age_groups_low$Age[i]
  ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_age_group_low (id) VALUES ({value})", .con = db_conn)
  DBI::dbExecute(db_conn, ins_stmt)
}

# -----------------------------------------------------------------------------

df_reg_durs <- df_de_age_duree %>%
  distinct(Duree_inscription) %>%
  mutate(
    Duree_inscription = stringr::str_replace_all(Duree_inscription, "\\s+", ""),  # remove all whitespace characters
    Duree_inscription = stringr::str_trim(Duree_inscription)                      # remove leading/trailing just in case
  ) %>%
  filter(Duree_inscription != "", !is.na(Duree_inscription))                      # remove empty values and NA values

for (i in 1:nrow(df_reg_durs)) {
  value = df_reg_durs$Duree_inscription[i]
  ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_reg_dur (id) VALUES ({value})", .con = db_conn)
  DBI::dbExecute(db_conn, ins_stmt)
}

# -----------------------------------------------------------------------------

df_reg_durs_low <- df_de_profil %>%
  distinct(Duree_d_inscription) %>%
  mutate(
    Duree_d_inscription = stringr::str_replace_all(Duree_d_inscription, "\\s+", ""),  # remove all whitespace characters
    Duree_d_inscription = stringr::str_trim(Duree_d_inscription)                      # remove leading/trailing just in case
  ) %>%
  filter(Duree_d_inscription != "", !is.na(Duree_d_inscription))                      # remove empty values and NA values

for (i in 1:nrow(df_reg_durs_low)) {
  value = df_reg_durs_low$Duree_d_inscription[i]
  ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_reg_dur_low (id) VALUES ({value})", .con = db_conn)
  DBI::dbExecute(db_conn, ins_stmt)
}

# -----------------------------------------------------------------------------

df_nats <- df_de_nationalite %>%
  distinct(Nationalite) %>%
  mutate(
    Nationalite = stringr::str_replace_all(Nationalite, "\\s+", ""),  # remove all whitespace characters
    Nationalite = stringr::str_trim(Nationalite)                      # remove leading/trailing just in case
  ) %>%
  filter(Nationalite != "", !is.na(Nationalite))                      # remove empty values and NA values

for (i in 1:nrow(df_nats)) {
  value = df_nats$Nationalite[i]
  ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_nat (id) VALUES ({value})", .con = db_conn)
  DBI::dbExecute(db_conn, ins_stmt)
}

# -----------------------------------------------------------------------------

df_educ_levels <- df_de_profil %>%
  distinct(Niveau_de_diplome) %>%
  mutate(
    Niveau_de_diplome = stringr::str_replace_all(Niveau_de_diplome, "\\s+", ""),  # remove all whitespace characters
    Niveau_de_diplome = stringr::str_trim(Niveau_de_diplome)                      # remove leading/trailing just in case
  ) %>%
  filter(Niveau_de_diplome != "", !is.na(Niveau_de_diplome))                      # remove empty values and NA values

for (i in 1:nrow(df_educ_levels)) {
  value = df_educ_levels$Niveau_de_diplome[i]
  ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_educ_level (id) VALUES ({value})", .con = db_conn)
  DBI::dbExecute(db_conn, ins_stmt)
}

# -----------------------------------------------------------------------------

df_res <- df_de_indemnite %>%
  distinct(Residence) %>%
  mutate(
    Residence = stringr::str_replace_all(Residence, "\\s+", ""),  # remove all whitespace characters
    Residence = stringr::str_trim(Residence)                      # remove leading/trailing just in case
  ) %>%
  filter(Residence != "", !is.na(Residence))                      # remove empty values and NA values

for (i in 1:nrow(df_res)) {
  value = df_res$Residence[i]
  ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_res (id) VALUES ({value})", .con = db_conn)
  DBI::dbExecute(db_conn, ins_stmt)
}

# -----------------------------------------------------------------------------

df_mesures <- df_de_mesure %>%
  distinct(Mesure) %>%
  mutate(
    Mesure = stringr::str_replace_all(Mesure, "\\s+", ""),  # remove all whitespace characters
    Mesure = stringr::str_trim(Mesure)                      # remove leading/trailing just in case
  ) %>%
  filter(Mesure != "", !is.na(Mesure))                      # remove empty values and NA values

for (i in 1:nrow(df_mesures)) {
  value = df_mesures$Mesure[i]
  ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_mesure (id) VALUES ({value})", .con = db_conn)
  DBI::dbExecute(db_conn, ins_stmt)
}

# -----------------------------------------------------------------------------

df_contract_types <- df_offre_detail %>%
  distinct(Nature_contrat) %>%
  mutate(
    Nature_contrat = stringr::str_replace_all(Nature_contrat, "\\s+", ""),  # remove all whitespace characters
    Nature_contrat = stringr::str_trim(Nature_contrat)                      # remove leading/trailing just in case
  ) %>%
  filter(Nature_contrat != "", !is.na(Nature_contrat))                      # remove empty values and NA values

for (i in 1:nrow(df_contract_types)) {
  value = df_contract_types$Nature_contrat[i]
  ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_contract_type (id) VALUES ({value})", .con = db_conn)
  DBI::dbExecute(db_conn, ins_stmt)
}

# -----------------------------------------------------------------------------

df_offre_detail[] <- lapply(df_offre_detail, function(col) {
  if (is.character(col)) {
    stri_trans_general(col, "Latin-ASCII")
  } else {
    col
  }
})

df_metiers <- df_offre_detail %>%
  distinct(ROME_metier, .keep_all = TRUE) %>%
  mutate(
    ROME_metier = stringr::str_replace_all(ROME_metier, "\\s+", ""),  # remove all whitespace characters
    ROME_metier = stringr::str_trim(ROME_metier)                      # remove leading/trailing just in case
  ) %>%
  filter(ROME_metier != "", !is.na(ROME_metier)) %>%                      # remove empty values and NA values
  select(ROME_metier, ROME_metier_libelle, ROME_niveau1, ROME_niveau1_libelle, ROME_niveau2, ROME_niveau2_libelle, ROME_Appellation)

# not necessary, following the above:
# df_metiers_metadata <- df_offre_detail %>%
#   select(ROME_metier, ROME_metier_libelle, ROME_niveau1, ROME_niveau1_libelle, ROME_niveau2, ROME_niveau2_libelle, ROME_Appellation) %>%
#   filter(ROME_metier %in% df_metiers$ROME_metier) %>%
#   distinct(ROME_metier)

# causes crash of RStudio while inserting only the first record: -- maybe because of unexpected character encoding on .csv file / data frame
# for (i in 1:nrow(df_metiers)) {
#   db_conn <- connect_db()
#   ins_stmt = glue::glue_sql("INSERT INTO student_miguel.kef_metier (id, descr, rome_n1, rome_n1_desc, rome_n2, rome_n2_desc, rome_appellation)
#                             VALUES
#                             ({df_metiers$ROME_metier[i]},
#                             {df_metiers$ROME_metier_libelle[i]},
#                             {df_metiers$ROME_niveau1[i]},
#                             {df_metiers$ROME_niveau1_libelle[i]},
#                             {df_metiers$ROME_niveau2[i]},
#                             {df_metiers$ROME_niveau2_libelle[i]},
#                             {df_metiers$ROME_Appellation[i]})", .con = db_conn)
#   DBI::dbExecute(db_conn, ins_stmt)
#   DBI::dbDisconnect(db_conn)
# }

colnames(df_metiers) <- c("id", "descr", "rome_n1", "rome_n1_desc", "rome_n2", "rome_n2_desc", "rome_appellation")

rows_inserted <- RPostgres::dbAppendTable(db_conn, DBI::Id(schema = "student_miguel", table = "kef_metier"), df_metiers)


# -----------------------------------------------------------------------------

DBI::dbDisconnect(db_conn)



