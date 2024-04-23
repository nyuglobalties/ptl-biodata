## Add globals from environment variables here
F_RUN_TESTS <- as.logical(Sys.getenv("F_RUN_TESTS") %if_empty_string% "FALSE")
