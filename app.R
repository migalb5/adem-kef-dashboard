

library(shiny)
library(bslib)

# library(purrr)
# library(tibble)
# library(DT)

library(shinycssloaders)




filterByYearMonth <- selectInput(
  inputId = "current_date",
  label = "Choose a year/month:",
  choices = c(1,2,3)
)

filterReferenceYearMonth <- selectInput(
  inputId = "compare_date",
  label = "Choose a year/month as reference (to compare with):",
  choices = c(1,2,3,4)
)


#cards <- list(
#   card(
#     full_screen = TRUE,
#     #card_header("Filters"),
#     layout_sidebar(
#       sidebar = sidebar("Filters", filterByYearMonth, filterByReferenceYearMonth),
#       withSpinner(DTOutput("de_genre"))
#     )
#   )
# )


ui <- page_sidebar(
  title = "ADEM KEF Dashboard",

  # Define theme
  theme = bs_theme(version = 5, bootswatch = "minty"),

  # Sidebar content (shared across all tabs)
  sidebar = sidebar(
    h4("Filters"),
    filterByYearMonth,
    filterByReferenceYearMonth
  ),

  # Main body with tabs
  navs_tab(
    nav_panel("Demandeurs d'Emploi",
              h3("Welcome to the Home Tab"),
              textOutput("home_text")
    ),
    nav_panel("Demandeurs d'Emploi Indemnisés",
              h3("Analytics Section"),
              plotOutput("plot")
    ),
    nav_panel("Offres d'Emploi",
              h3("Reports Section"),
              verbatimTextOutput("report")
    ),
    nav_panel("Correlations",
              h3("Settings"),
              checkboxInput("dark_mode", "Enable dark mode")
    )
  )
)

server <- function(input, output, session) {
  output$home_text <- renderText({
    paste("You selected:", input$select)
  })

  output$plot <- renderPlot({
    hist(rnorm(100), main = "Random Histogram")
  })

  output$report <- renderPrint({
    summary(mtcars)
  })

  observe({
    if (input$dark_mode) {
      session$setCurrentTheme(bs_theme(version = 5, bootswatch = "darkly"))
    } else {
      session$setCurrentTheme(bs_theme(version = 5, bootswatch = "minty"))
    }
  })
}

# server <- function(input, output) {
#
#   de_genre <- reactive({
#     return(1)
#   })
#
#   output$de_genre <- renderDataTable({
#     datatable(de_genre(), filter = "top", rownames = FALSE)
#   })
# }

shinyApp(ui, server)


