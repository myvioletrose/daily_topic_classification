if(!require(stringr)){install.packages("stringr"); require(stringr)}
if(!require(dplyr)){install.packages("dplyr"); require(dplyr)}

who <- function(x) {
        str_split(x, "") %>%
                unlist %>%
                grep(pattern = "[a-z]", value = T) %>%
                str_c(collapse = "")
}

pwd <- function(x) {
        str_split(x, "") %>%
                unlist %>%
                grep(pattern = "[a-z0-9]", value = T) %>%
                str_c(collapse = "")
}