
run_data_pp <- function () {
  db_conn <- connect_db()

  df_trf <- extract_transform_table("student_miguel.kef_de_metier")
  update_table(db_conn, df_trf, "student_miguel.kef_de_metier")

  df_trf <- extract_transform_table("student_miguel.kef_de_profil")
  update_table(db_conn, df_trf, "student_miguel.kef_de_metier")

  df_trf <- extract_transform_table("student_miguel.kef_de_mesure")
  update_table(db_conn, df_trf, "student_miguel.kef_de_metier")

  df_trf <- extract_transform_table("student_miguel.kef_de_age_dur")
  update_table(db_conn, df_trf, "student_miguel.kef_de_metier")

  df_trf <- extract_transform_table("student_miguel.kef_de_res")
  update_table(db_conn, df_trf, "student_miguel.kef_de_metier")

  df_trf <- extract_transform_table("student_miguel.kef_de_nat")
  update_table(db_conn, df_trf, "student_miguel.kef_de_metier")

  df_trf <- extract_transform_table("student_miguel.kef_oe")
  update_table(db_conn, df_trf, "student_miguel.kef_de_metier")

  DBI::dbDisconnect(db_conn)
}
