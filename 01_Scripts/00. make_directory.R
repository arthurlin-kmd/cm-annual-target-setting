library(fs)

make_project_dir <- function() {
    
    dir_names <- c(
        "00_Data",
        "01_Scripts",
        "02_Output")
    
    dir_create(dir_names)
    
    dir_ls()
}
make_project_dir()
