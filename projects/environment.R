## Add globals from environment variables here
F_RUN_TESTS <- as.logical(Sys.getenv("F_RUN_TESTS") %if_empty_string% "FALSE")
F_RUN_ALL <- as.logical(Sys.getenv("F_RUN_ALL") %if_empty_string% "TRUE")

SYNOLOGY_ROOT <- Sys.getenv("SYNOLOGY_ROOT") %if_empty_string% NULL
