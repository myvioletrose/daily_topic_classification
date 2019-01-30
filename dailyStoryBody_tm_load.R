## set wd
setwd("C:/Users/traveler/Desktop/Jim/# R/# analytics/project/DailyStory_Project_417/R_setup/dailyStoryBody_tm")

rm(list = ls())

## set run time
run_time_start <- Sys.time()

## load packages
if(!require(bigrquery)){install.packages("bigrquery"); require(bigrquery)}
if(!require(rmarkdown)){install.packages("rmarkdown"); require(rmarkdown)}
if(!require(knitr)){install.packages("knitr"); require(knitr)}
if(!require(mailR)){install.packages("mailR"); require(mailR)}
if(!require(lubridate)){install.packages("lubridate"); require(lubridate)}
if(!require(dplyr)){install.packages("dplyr"); require(dplyr)}
if(!require(ggplot2)){install.packages("ggplot2"); require(ggplot2)}
if(!require(stringr)){install.packages("stringr"); require(stringr)}
if(!require(purrr)){install.packages("purrr"); require(purrr)}
if(!require(rlang)){install.packages("rlang"); require(rlang)}
if(!require(tidyverse)){install.packages("tidyverse"); require(tidyverse)}
if(!require(tidytext)){install.packages("tidytext"); require(tidytext)}
if(!require(rvest)){install.packages("rvest"); require(rvest)}
if(!require(plyr)){install.packages("plyr"); require(plyr)}
if(!require(tm)){install.packages("tm"); require(tm)}
if(!require(topicmodels)){install.packages("topicmodels"); require(topicmodels)}
if(!require(SnowballC)){install.packages("SnowballC"); require(SnowballC)}


# Big Query arguments
source("arguments.R")

# email recipients
source("recipients.R")


#############################################
###### step 1: get the body from html #######
###### step 2: clean up the messy text ######
######  step 3: LDA transform, extract ######
######## step 4: put things together ########
#############################################

#########################

# one_quote <- paste(unlist(csw), collapse = ", ")  # the custom stop words list from xlsx
# unquote <- one_quote %>% 
#        stringr::str_split(., pattern = ", ") %>%
#        unlist
# which(!custom_stop_words == unquote)  # check

# do not name any object as "stop_words" because it is already an object from the tidyverse that needs to be called later in the script
# one_quote <- paste(unlist(csw), collapse = ", ") %>%  # the csw (custom stop words) is just a list from xlsx manually updated
#         write.csv(., "clipboard", row.names = F)
# the result of one_quote is just copied and pasted here inside csw

### custom stop word list as of May 7, 2018 (list of 800 custom stop words) ###
# csw <- read.csv("clipboard", header = F); dim(csw); head(csw)
# 
# one_quote <- paste(unlist(csw), collapse = ", ") %>%  
#         # write.csv(., "clipboard", row.names = F) %>%
#         stringr::str_split(., pattern = ", ") %>%
#         unlist
# 
# custom_stop_words <- tibble(word = one_quote) %>%
#         print

# saveRDS(object = custom_stop_words, file = "custom_stop_words_5.7.2018.rds")
custom_stop_words <- readRDS(file = "custom_stop_words_5.7.2018.rds")

#################################################
################### functions ###################
#################################################

# extract body from html
extract_fr_html <- function(body) 
{
        body <- body %>%
                as.character %>%
                read_html %>%
                html_nodes(xpath = ".//p") %>%
                html_text %>%
                paste(., collapse = " ")
}

# use possibly to handle error, e.g. some story_ids either not have body or messy html
possibly_extract <- possibly(extract_fr_html, 
                             otherwise = NA_real_)

# clean text 
clean.text = function(x)
{
        # tolower
        x = tolower(x)
        # remove \n
        x = gsub("\n", "", x)
        # remove at
        x = gsub("@\\w+", "", x)
        # remove punctuation
        x = gsub("[[:punct:]]", "", x)
        # remove numbers
        x = gsub("[[:digit:]]", "", x)   
        # remove links http
        x = gsub("http\\w+", "", x)
        # remove tabs
        x = gsub("[ |\t]{2,}", "", x)
        # remove blank spaces at the beginning
        x = gsub("^ ", "", x)
        # remove blank spaces at the end
        x = gsub(" $", "", x)
        return(x)
}

# LDA - top terms by 4 topics 
# https://www.kaggle.com/rtatman/nlp-in-r-topic-modelling
top_terms_by_topic_LDA_2 <- function(input_text, # should be a columm from a dataframe
                                     number_of_topics = 4) # number of topics (4 by default)
{    
        # create a corpus (type of object expected by tm) and document term matrix
        Corpus <- Corpus(VectorSource(input_text)) # make a corpus object
        DTM <- DocumentTermMatrix(Corpus) # get the count of words/document
        
        # remove any empty rows in our document term matrix (if there are any 
        # we'll get an error when we try to run our LDA)
        unique_indexes <- unique(DTM$i) # get the index of each unique value
        DTM <- DTM[unique_indexes,] # get a subset of only those indexes
        
        # preform LDA & get the words/topic in a tidy text format
        lda <- LDA(DTM, k = number_of_topics, control = list(seed = 1234))
        topics <- tidy(lda, matrix = "beta")
        
        # get the top ten terms for each topic
        top_terms <- topics  %>% # take the topics data frame and..
                group_by(topic) %>% # treat each topic as a different group
                top_n(10, beta) %>% # get the top 10 most informative words
                ungroup() %>% # ungroup
                # arrange(topic, -beta) %>% # arrange words in descending informativeness
                arrange(desc(beta)) %>%
                head %>%
                select(term) %>%
                unique %>%
                unlist %>%
                paste(., collapse = ", ") 
        
        return(top_terms)
}


##################################################
################# set parameters #################

# get the last job number from [media-data-science:Jim_Story_Audit.topic_classification_log]
last_jb <- query_exec(query = "select max(job_number) from [media-data-science:Jim_Story_Audit.topic_classification_log] where flag = 1",
                      project = projectId,
                      use_legacy_sql = T) %>% 
        as.integer
# get the current jb
jb <- last_jb + 1 %>%
        as.integer
# get the input count from the current job number
input_count <- query_exec(query = sprintf("select sum(distinct_story_load) from [media-data-science:Jim_Story_Audit.story_body_audit_a] where job_number >= %d", jb),
                 project = projectId,
                 use_legacy_sql = T) %>%
        as.integer
# in case of running for multiple jobs, get the maximum jb number for logging purposes
current_max_jb <- query_exec(query = sprintf("select max(job_number) from [media-data-science:Jim_Story_Audit.story_body_audit_a] where job_number >= %d", jb),
                 project = projectId,
                 use_legacy_sql = T) %>%
        as.integer        


###############################
# write query and save result #
body_start <- Sys.time()
j_sql <- sprintf(
    "
    SELECT story_id, body
    FROM [media-data-science:Jim_Story_Production.story_body_b]
    where job_number >= %d
    "
, jb)

j_body <- query_exec(query = j_sql,
                     project = projectId,
                     use_legacy_sql = T, 
                     max_pages = Inf)  # very important to set this up to read all pages

body_end <- Sys.time()
body_run_time <- body_end - body_start

input <- dim(j_body)[1]


##################################################
############### set up input logic ###############

# if input not found, terminate the job immediately
if(!exists("input")){
    source("pwd.R")
    email <- mailR::send.mail(from = paste0(who(sender), "@gmail.com"),
                              to = recipients,
                              subject = sprintf("FAILURE_daily_loading_tm: NO INPUT - job number(s) from %d to %d", jb, current_max_jb),
                              body = "Error: An internal error occurred and the request could not be completed.",
                              smtp = list(host.name = "smtp.gmail.com", 
                                          port = 465, 
                                          user.name = who(username),
                                          passwd = pwd(password),
                                          ssl = T),
                              authenticate = T,
                              send = T
                              # attach.files = c(grep(pattern = "top_articles_output|dist_published_date|dist_primary_site|top_10_words_from_each_site|wordCloud_top_articles", 
                              # dir(), 
                              # value = T))
    )
    rm(sender, username, password, who, pwd)
    stop("check out this Error: An internal error occurred and the request could not be completed.")
    quit(save = "no")
}

# if input is not matching with input count, or input is 0, terminate the job immediately 
if(xor(!(input == input_count), input == 0)){
    source("pwd.R")
    email <- mailR::send.mail(from = paste0(who(sender), "@gmail.com"),
                              to = recipients,
                              subject = "ERROR_daily_loading_tm: xor(!(input == input_count), input == 0)",
                              body = "xor(!(input == input_count), input == 0), i.e. either the input is not matching with the input count, or input is equal to 0",
                              smtp = list(host.name = "smtp.gmail.com", 
                                          port = 465, 
                                          user.name = who(username),
                                          passwd = pwd(password),
                                          ssl = T),
                              authenticate = T,
                              send = T
                              # attach.files = c(grep(pattern = "top_articles_output|dist_published_date|dist_primary_site|top_10_words_from_each_site|wordCloud_top_articles", 
                              # dir(), 
                              # value = T))
    )
    rm(sender, username, password, who, pwd)
    stop("check out this Error: xor(!(input == input_count), input == 0)")
    quit(save = "no")
}

#############################################################################
##############################################
######## put step 1, 2, 3, 4 together ########
##############################################

# run LDA here #
# below process will break if and only if ALL input story_ids messed up, such as missing body 

start <- Sys.time()

j <- j_body %>%
        # filter(type == "article") %>%
        # head %>%
        # filter(story_id = 'NGMWE46K50YA01') %>%
        select(story_id, body) %>%
        mutate(clean_body = map(body, 
                                possibly_extract) %>%  # extract body from html, use possibly to handle error
                       unlist) %>%
        mutate(clean_body = clean.text(clean_body)) %>%  # clean up the messy body
        select(story_id, clean_body) %>%
        unnest_tokens(output = token, 
                      input = clean_body) %>%  # unnest (into token) for removing stop_words and custom_stop_words
        na.omit %>%  # remove any <NA>, some story_ids either not have a body or something messed up in the html 
        anti_join(stop_words, by = c("token" = "word")) %>%  # remove stop_words
        anti_join(custom_stop_words, by = c("token" = "word")) %>%
        # mutate(stem = SnowballC::wordStem(token, language = "english")) %>%  # stemming
        # select(story_id, stem) %>%
        # nest(stem) %>%  # nest it back 
        select(story_id, token) %>%
        nest(token) %>%  # nest it back
        mutate(text = map(data, unlist),
               text = map_chr(text, paste, collapse = " ")) %>%  # piece together the list of tokens after clean-up 
        select(story_id, text) %>%
        mutate(topic = map(text,
                           function(x) {
                                   top_terms_by_topic_LDA_2(x,
                                                            number_of_topics = 4)  # Latent Dirichlet Allocation (LDA)
                           })) %>%  
        select(story_id, topic) %>%
        mutate(topic = topic %>% 
                       unlist %>%
                       as.vector) %>%  # convert the "list" into "vector" so that it can be unnested into a single column
        arrange(story_id) %>%
        unnest_tokens(token, topic) %>%  # unnest "topic" into a single column of "token"
        mutate(flag = nchar(token)) %>%
        filter(flag >=3) %>%  # remove any token that has just 1 or 2 letters
        select(story_id, token)

end <- Sys.time()
LDA_run_time <- end - start

dim(j)

#############################################################################
#############################################################################
# insert upload to Big Query

# input <- dim(j_body)[1]
loaded <- unique(j$story_id) %>%
        as.vector %>%
        length

load_diff <- abs(input - loaded)
missing <- paste0(load_diff, " stories missing. ", sep = "")

flag <- if((load_diff / input) == 0){
        1
} else {
        if((load_diff / input) > 0.005){
                0
        } else {
                1
        }
}

run_time <- str_subset(ls(), 
                       pattern = "body_run_time|LDA_run_time") %>%
        # print %>%
        as.list %>%
        map(., function(x){eval(rlang::sym(x))}) %>%
        # print %>%
        map(., function(x){
                str_extract_all(x, "[0-9]+\\.[0-9]+") %>%
                        # str_replace_all(pattern = " secs", replacement = "") %>%
                        as.numeric %>%
                        round(., 2)
        })

# load it in staging
insert_upload_job(values = j,
                  project = projectId,
                  dataset = dataset_staging,
                  table = "topic_classification_a",
                  write_disposition = "WRITE_TRUNCATE"
) %>%
        invisible %>%
        wait_for(quiet = F, pause = 0.5)

# check topic_classification_a from staging
check_sql <- "
select count(1) as flag
from (
SELECT story_id, token, count(1) as flag
FROM [media-data-science:Jim_Story_Staging.topic_classification_a] 
group by 1, 2
having count(1) >1
) x
" 

duplication <- query_exec(query = check_sql,
                    project = projectId,
                    use_legacy_sql = T) %>%
        as.integer

# load it into production if passing the check
if(isTRUE(flag == 1 & duplication == 0)){
        insert_upload_job(values = j,
                          project = projectId,
                          dataset = dataset_production,
                          table = "topic_classification_b",
                          write_disposition = "WRITE_APPEND"
        ) %>%
                invisible %>%
                wait_for(quiet = F, pause = 0.5)  
} else {
  flag <- 0
}

run_time_end <- Sys.time()

####################
# load it into log #
logDb <- data.frame(job_number = if(flag == 1){ current_max_jb } else { jb },
                    run_time_start,
                    run_time_end,
                    body_run_time = run_time[[1]],
                    LDA_run_time = run_time[[2]],
                    input = input_count,
                    output = as.integer(loaded),
                    flag = as.integer(flag))

insert_upload_job(values = logDb,
                  project = projectId,
                  dataset = dataset_audit,
                  table = "topic_classification_log",
                  write_disposition = "WRITE_APPEND"
) %>%
        invisible %>%
        wait_for(quiet = F, pause = 0.5)


#######################################
## set up email message

jb  # job number
current_max_jb  # current job number
input_count  # input
loaded  # output
flag  # red flag

missing  # number of missing stories
body_run_time <- paste0("body run time: ", round(body_run_time, 1), ". ")
LDA_run_time <- paste0("LDA run time: ", round(LDA_run_time, 1), ". ")

message <- paste0(sprintf("The current job number is %d (to %d). \nThe input is %d, whereas the output is %d. \nFlag is %d. \n", 
                          jb, current_max_jb, input_count, loaded, flag), 
                  missing, "\n", body_run_time, "\n", LDA_run_time, 
                  sep = "\n\n")


########################
## email job complete ##

if(flag == 1){
    source("pwd.R")
    email <- mailR::send.mail(from = paste0(who(sender), "@gmail.com"),
                              to = recipients,
                              subject = sprintf("COMPLETE_daily_loading_tm: job %d to %d", jb, current_max_jb),
                              body = message,
                              smtp = list(host.name = "smtp.gmail.com", 
                                          port = 465, 
                                          user.name = who(username),
                                          passwd = pwd(password),
                                          ssl = T),
                              authenticate = T,
                              send = T
                              # attach.files = c(grep(pattern = "top_articles_output|dist_published_date|dist_primary_site|top_10_words_from_each_site|wordCloud_top_articles", 
                              # dir(), 
                              # value = T))
    )
    rm(sender, username, password, who, pwd)
} else {
    source("pwd.R")
    email <- mailR::send.mail(from = paste0(who(sender), "@gmail.com"),
                              to = recipients,
                              subject = sprintf("ALERT_daily_loading_tm: job %d to %d", jb, current_max_jb),
                              body = paste0(message, "There is a red flag for this job. Either more than 0.5% of inputs are missing, or there's duplication found for the output (of LDA) in staging. Please resolve it asap before moving on to the next job. Data is in staging, not moving to production yet.", sep = "\n\n"),
                              smtp = list(host.name = "smtp.gmail.com", 
                                          port = 465, 
                                          user.name = who(username),
                                          passwd = pwd(password),
                                          ssl = T),
                              authenticate = T,
                              send = T
                              # attach.files = c(grep(pattern = "top_articles_output|dist_published_date|dist_primary_site|top_10_words_from_each_site|wordCloud_top_articles", 
                              # dir(), 
                              # value = T))
    )
    rm(sender, username, password, who, pwd)
    # quit(save = "no")
}

