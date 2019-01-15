## set wd
setwd("C:/Users/traveler/Desktop/Jim/# R/# analytics/project/DailyStory_Project_417/R_setup")

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

# Big Query arguments
projectId <- "media-data-science"
dataset_staging <- "Jim_Story_Staging"
dataset_production <- "Jim_Story_Production"
dataset_audit <- "Jim_Story_Audit"
# table_id <- "audit_a"

# email recipients
recipients <- c("myvioletrose@gmail.com") 

####################################
# email - subjects, bodies
error_subject_1 <- "ERROR_DailyStoryJob: Error1@audit_a"
error_body_1 <- "Error1: either 'last_run_sys_time' is equal to 'current_run_time' and/or last flag is equal to 0, please check the audit_a table"

error_subject_2 <- "ERROR_DailyStoryJob: Error2@audit_a"
error_body_2 <- "Error2: there is a red flag for the last entry in the audit_a table, please check it"

error_subject_3 <- "ERROR_DailyStoryJob: Error3@audit_b"
error_body_3 <- "Error3: either a datekey is associated with more than one job number, and/or a red flag is raised for the last entry"

error_subject_4 <- "ERROR_DailyStoryJob: Error4@story_body_a (staging)"
error_body_4 <- "Error4: there is duplication found for story Id(s) in the story_body_a (staging) table"

error_subject_5 <- "ERROR_DailyStoryJob: Error5@story_body_audit_a"
error_body_5 <- "Error5: there is a red flag for the last entry in the story_body_audit_a table, please check it"

success_subject <- "SUCCESS_DailyStoryJob"
success_body <- "the daily job is run successfully, please see below log"

################################################################
####### step (0) check audit_a ##################
## objectives
# 1) extract max date to set parameter
# 2) do not run the job if "last_run_sys_time" is equal to "current_run_time"
# 3) do not continue the job if last flag is equal to 0

## Big Query
# use_Legacy_sql = T 

check_audit_a_query <- "
select max_date as last_entry_time
, sys_run_time as last_run_sys_time
, DATE_ADD(CURRENT_DATE(), 0, 'DAY') as current_run_time
, flag 
from [media-data-science:Jim_Story_Audit.audit_a]
where job_number = (select max(job_number) from [media-data-science:Jim_Story_Audit.audit_a] where flag = 1)
" 
        
# write query and save result
audit_a_maxdate <- query_exec(query = check_audit_a_query,
                   project = projectId,
                   use_legacy_sql = T) 

last_entry_time <- ymd(audit_a_maxdate$last_entry_time)
last_run_sys_time <- as.Date(audit_a_maxdate$last_run_sys_time)
current_run_time <- ymd(audit_a_maxdate$current_run_time)
# flag <- audit_a_maxdate$flag

# set parameter: number of days that we need to go back to run for our job
diff <- difftime(current_run_time, last_entry_time, units = 'days') %>% 
        str_extract(pattern = "[0-9]") %>%
        as.integer %>%
        -1

################################################################
####################### run logic_A here #######################
red_flag <- if(xor(last_run_sys_time == current_run_time, last_entry_time == current_run_time)) {
    1
    } else {
        0
    }

if(red_flag == 1){
        source("pwd.R")
        email <- mailR::send.mail(from = paste0(who(sender), "@gmail.com"),
                                  to = recipients,
                                  subject = error_subject_1,
                                  body = error_body_1,
                                  smtp = list(host.name = "smtp.gmail.com", 
                                              port = 465, 
                                              user.name = who(username),
                                              passwd = pwd(password),
                                              ssl = T),
                                  authenticate = T,
                                  send = T
                                  # attach.files = c(grep(pattern = "top_articles_output|dist_published_date|dist_primary_site|top_10_words_from_each_site|wordCloud_top_articles", 
                                  #                       dir(), 
                                  #                       value = T))
        )
        rm(sender, username, password, who, pwd)
        stop("check out this Error_1")
        quit(save = "no")
}

##################################################################
######### step (1) truncate daily_story_a ########
## objectives
# 1) truncate and insert data into staging table

## Big Query
# write_disposition = "WRITE_TRUNCATE"
# use_legacy_sql = T

truncate_daily_story_a_query <- sprintf(
        "select a.date as date
        , a.country as country
        , a.city as city
        , a.latitude as latitude
        , a.longitude as longitude
        , a.functional_area as functional_area
        , a.uvs as uvs 
        , a.uvs_by_fa as uvs_by_fa 
        , a.story_id as story_id 
        --, s.authors as authors 
        , s.primary_site as primary_site
        , s.published_at as published_at
        , s.headline as headline
        , a.rank as daily_rank
        , current_timestamp() as sys_run_time
        
        FROM (
        select x.date as date
        , x.country as country
        , x.city as city
        , x.latitude as latitude
        , x.longitude as longitude 
        , x.story_id as story_id
        , x.uvs as uvs
        , x.rank as rank
        , case when z.functional_area is null then 'n/a' else z.functional_area end as functional_area
        , z.uvs_by_fa as uvs_by_fa 
        
        from (
        
        select date, country, city, latitude, longitude, story_id, uvs
        , row_number() over (partition by date, country, city, latitude, longitude order by uvs desc) as rank
        
        from (
        select date 
        , geoNetwork.country as country
        , geoNetwork.city as city
        , geoNetwork.latitude as latitude
        , geoNetwork.longitude as longitude
        , hits.customDimensions.value as story_id
        , count(distinct fullVisitorId) as uvs
        from TABLE_DATE_RANGE([calcium-land-150922:132466937.ga_sessions_], 
        --USEC_TO_TIMESTAMP(UTC_USEC_TO_MONTH(current_date())), 	
        --DATE_ADD(CURRENT_DATE(), -1, 'DAY'), 
        DATE_ADD(CURRENT_DATE(), -%d, 'DAY'), 
        DATE_ADD(CURRENT_DATE(), 0, 'DAY'))		
        where hits.type = 'PAGE'
        and hits.customDimensions.index = 7
        group by 1, 2, 3, 4, 5, 6
        ) r1
        where story_id is not null 
        
        ) x
        
        left join (
        
        select xx.date as date
        , xx.country as country
        , xx.city as city
        , xx.story_id as story_id
        , yy.functional_area as functional_area
        , count(distinct xx.fullVisitorId) as uvs_by_fa
        
        from (
        
        select fullVisitorId
        , date
        , geoNetwork.country as country
        , geoNetwork.city as city
        , hits.customDimensions.value as story_id
        from TABLE_DATE_RANGE([calcium-land-150922:132466937.ga_sessions_], 
        --USEC_TO_TIMESTAMP(UTC_USEC_TO_MONTH(current_date())), 	
        --DATE_ADD(CURRENT_DATE(), -1, 'DAY'), 
        DATE_ADD(CURRENT_DATE(), -%d, 'DAY'), 
        DATE_ADD(CURRENT_DATE(), 0, 'DAY'))		
        where hits.type = 'PAGE'
        and hits.customDimensions.index = 7
        group by 1, 2, 3, 4, 5
        
        ) xx 
        
        join (
        
        select a.fullVisitorId as fullVisitorId
        , a.date as date
        , case when b.functional_area is null then 'n/a' else b.functional_area end as functional_area
        
        from (
        
        select fullVisitorId, date 
        , max(hits.customDimensions.value) as bloomberg_id 
        
        from TABLE_DATE_RANGE([calcium-land-150922:132466937.ga_sessions_], 
        --USEC_TO_TIMESTAMP(UTC_USEC_TO_MONTH(current_date())), 		
        --DATE_ADD(CURRENT_DATE(), -1, 'DAY'), 
        DATE_ADD(CURRENT_DATE(), -%d, 'DAY'), 
        DATE_ADD(CURRENT_DATE(), 0, 'DAY'))		
        where hits.type = 'PAGE'
        and hits.customDimensions.index = 59
        group by 1, 2
        
        ) a
        
        left join [media-data-science:Jim_Story_Production.bmb_functional_area] b on a.bloomberg_id = b.bloomberg_id
        
        group by 1, 2, 3
        
        ) yy on xx.fullVisitorId = yy.fullVisitorId and xx.date = yy.date 
        
        where yy.functional_area != 'n/a' 
        
        group by 1, 2, 3, 4, 5
        
        ) z on x.date = z.date and x.country = z.country and x.city = z.city and x.story_id = z.story_id
        
        ) a
        
        LEFT JOIN [calcium-land-150922:datalake.story_metadata] s on a.story_id = s.id", 
        diff, diff, diff)

start1 <- Sys.time()
step1 <- insert_query_job(truncate_daily_story_a_query, 
                          project = projectId,
                          destination_table = list(
                                  project_id = projectId,
                                  dataset_id = dataset_staging,
                                  table_id = "daily_story_a"),
                          # destination_table = "media-data-science:Jim_Story_Staging.daily_story_a",
                          write_disposition = "WRITE_TRUNCATE",
                          use_legacy_sql = T)
wait_for(job = step1, quiet = FALSE, pause = 0.5)
end1 <- Sys.time()
step1_run_time <- paste0("step 1 'truncate_daily_story_a_query' took ", 
                        round(difftime(end1, start1, units = 'secs'), 2),
                        " seconds")


#####################################################################
############# step (2) append audit_a #######################
## objectives
# 1) log into the audit table
# 2) red flag or not

## Big Query
# write_disposition = "WRITE_APPEND"
# use_legacy_sql = F

append_audit_a_query <- "
with x
as (
    select max(job_number) as max_job_number
    from `media-data-science.Jim_Story_Audit.audit_a`
)

select x.max_job_number +1 as job_number
--, PARSE_TIMESTAMP('%Y%m%d', y.max_date) as max_date 
, y.max_date as max_date
, y.sys_run_time as sys_run_time
, y.entry as entry 
, case when y.entry > 5000000 or y.entry < 500000 then 0 else 1 end as flag  -- if more than 5M or less than 0.5M should raise a red flag of 0 (whereas the value of 1 stands for job success)

from x
cross join (
    select max(date) as max_date
    , max(sys_run_time) as sys_run_time
    , count(1) as entry 
    FROM `media-data-science.Jim_Story_Staging.daily_story_a`  -- a staging table
) y
"

start2 <- Sys.time()
step2 <- insert_query_job(append_audit_a_query, 
                          project = projectId,
                          destination_table = list(
                                  project_id = projectId,
                                  dataset_id = dataset_audit,
                                  table_id = "audit_a"),
                          # destination_table = "media-data-science:Jim_Story_Audit.audit_a",
                          write_disposition = "WRITE_APPEND",  # very important to do just "APPEND"
                          use_legacy_sql = F)
wait_for(job = step2, quiet = FALSE, pause = 0.5)
end2 <- Sys.time()
step2_run_time <- paste0("step 2 'append_audit_a_query' took ", 
                        round(difftime(end2, start2, units = 'secs'), 2),
                        " seconds")


################################################################
####################### run logic_B here #######################
red_flag2 <- query_exec(query = "select flag from [media-data-science:Jim_Story_Audit.audit_a] 
                                where job_number = (select max(job_number) from [media-data-science:Jim_Story_Audit.audit_a])",
                       project = projectId,
                       use_legacy_sql = T) %>% 
        as.integer

if(red_flag2 == 0){
    source("pwd.R")
    email <- mailR::send.mail(from = paste0(who(sender), "@gmail.com"),
                              to = recipients,
                              subject = error_subject_2, 
                              body = error_body_2,
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
    stop("check out this Error_2")
    quit(save = "no")
}

################################################################
############# step (3) append daily_story_b ################
## objective
# 1) insert into production

## Big Query
# write_disposition = "WRITE_APPEND"
# use_legacy_sql = T

append_daily_story_b_query <- "
select cast(date as integer) as date
, country
, city
, latitude
, longitude
, functional_area
, uvs 
, uvs_by_fa 
, story_id 
, primary_site
, published_at
, headline
, daily_rank
, sys_run_time

from [media-data-science:Jim_Story_Staging.daily_story_a]
"
start3 <- Sys.time()
step3 <- insert_query_job(append_daily_story_b_query, 
                          project = projectId,
                          destination_table = list(
                                  project_id = projectId,
                                  dataset_id = dataset_production,
                                  table_id = "daily_story_b"),
                          # destination_table = "media-data-science:Jim_Story_Production.daily_story_b",
                          write_disposition = "WRITE_APPEND",  # very important to do just "APPEND"
                          use_legacy_sql = T)
wait_for(job = step3, quiet = FALSE, pause = 0.5)
end3 <- Sys.time()
step3_run_time <- paste0("step 3 'append_daily_story_b_query' took ", 
                        round(difftime(end3, start3, units = 'secs'), 2),
                        " seconds")


#####################################################################
############# step (4) truncate audit_b #######################
## objectives
# 1) log into the audit table
# 2) job success or not

## Big Query
# write_disposition = "WRITE_TRUNCATE"
# use_legacy_sql = F

truncate_audit_b_query <- "
with B 
as (
    select case when aa.job_number is null then 0 else aa.job_number end as job_number
    , b.date as date_key
    , b.sys_run_time as sys_run_time
    , b.entry as entry 

    from (
        select date
        , sys_run_time
        , count(1) as entry 
        from `media-data-science.Jim_Story_Production.daily_story_b`
        group by 1, 2
        order by 1
    ) b 

    left join `media-data-science.Jim_Story_Audit.audit_a` aa on b.sys_run_time = aa.sys_run_time 

    order by 1, 2
) 

select B.job_number as job_number
, B.date_key as date_key
, B.sys_run_time as sys_run_time
, B.entry as entry
, case when X.job_number is null then 0 else X.success end as job_success

from B 
join (
    select bb.job_number as job_number
    , case when aa.job_number is null then 0 
            when bb.entry = aa.entry then 1 else 0 
            end as success
    from (
        select B.job_number as job_number
        , sum(B.entry) as entry
        from B 
        group by 1
    ) bb  

    left join (
        select a.job_number as job_number 
        , sum(entry) as entry 
        from `media-data-science.Jim_Story_Audit.audit_a` a 
        group by 1
    ) aa on bb.job_number = aa.job_number
) X on B.job_number = X.job_number

order by 1, 2
"

start4 <- Sys.time()
step4 <- insert_query_job(truncate_audit_b_query, 
                          project = projectId,
                          destination_table = list(
                                  project_id = projectId,
                                  dataset_id = dataset_audit,
                                  table_id = "audit_b"),
                          # destination_table = "media-data-science:Jim_Story_Audit.audit_b",
                          write_disposition = "WRITE_TRUNCATE", 
                          use_legacy_sql = F)
wait_for(job = step4, quiet = FALSE, pause = 0.5)
end4 <- Sys.time()
step4_run_time <- paste0("step 4 'truncate_audit_b_query' took ", 
                        round(difftime(end4, start4, units = 'secs'), 2),
                        " seconds")


################################################################
####################### run logic_C here #######################
## check audit_b
# use_legacy_sql = F
check_audit_b_query <- "
select sum(red_flag) as red_flag
FROM (

  SELECT sum(case when job_success = 1 then 0 else 1 end) as red_flag
  FROM `media-data-science.Jim_Story_Audit.audit_b` 

  union all 

  select sum(case when job_number = 1 then 0 else 1 end) as red_flag
  from (
    SELECT date_key
    , count(job_number) as job_number
    FROM `media-data-science.Jim_Story_Audit.audit_b`
    group by 1
  ) x
 
) X
"
red_flag3 <- query_exec(query = check_audit_b_query,
                        project = projectId,
                        use_legacy_sql = F) %>% 
        as.integer

if(red_flag3 > 0){
        source("pwd.R")
        email <- mailR::send.mail(from = paste0(who(sender), "@gmail.com"),
                                  to = recipients,
                                  subject = error_subject_3, 
                                  body = error_body_3,
                                  smtp = list(host.name = "smtp.gmail.com", 
                                              port = 465, 
                                              user.name = who(username),
                                              passwd = pwd(password),
                                              ssl = T),
                                  authenticate = T,
                                  send = T
                                  # attach.files = c(grep(pattern = "top_articles_output|dist_published_date|dist_primary_site|top_10_words_from_each_site|wordCloud_top_articles", dir(), value = T))
        )
        rm(sender, username, password, who, pwd)
        stop("check out this Error_3")
        quit(save = "no")
}

################################################################
############ step (5) truncate story_body_a ###############
## objectives
# 1) load distinct stories from staging
# 2) check against production, i.e. story_body_b

## Big Query
# write_disposition = "WRITE_TRUNCATE"
# use_legacy_sql = F

truncate_story_body_a_query <- "
select s.id as story_id 
, s.type as type
, s.ga_type as ga_type
, s.primary_site as primary_site 
, s.published_at as published_at
, s.headline as headline
, s.summary as summary 
, s.body as body 
, audit.job_number as job_number 
from `media-data-science.Jim_Story_Staging.daily_story_a` a  -- staging
join `calcium-land-150922.datalake.story_metadata` s on a.story_id = s.id 
join `media-data-science.Jim_Story_Audit.audit_a` audit on a.sys_run_time = audit.sys_run_time
where s.id not in 
(
    select story_id
    from `media-data-science.Jim_Story_Production.story_body_b`  -- production
    group by 1
)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
"
start5 <- Sys.time()
step5 <- insert_query_job(truncate_story_body_a_query, 
                          project = projectId,
                          destination_table = list(
                                  project_id = projectId,
                                  dataset_id = dataset_staging,
                                  table_id = "story_body_a"),
                          # destination_table = "media-data-science:Jim_Story_Staging.story_body_a",
                          write_disposition = "WRITE_TRUNCATE",
                          use_legacy_sql = F)
wait_for(job = step5, quiet = FALSE, pause = 0.5)
end5 <- Sys.time()
step5_run_time <- paste0("step 5 'truncate_story_body_a_query' took ", 
                        round(difftime(end5, start5, units = 'secs'), 2),
                        " seconds")


################################################################
####################### run logic_D here #######################
## check story_body_a (staging)
# use_legacy_sql = F

check_story_body_a_query <- "
select sum(case when flag = 1 then 0 else flag end) as red_flag
from (
SELECT story_id, count(1) as flag
FROM `media-data-science.Jim_Story_Staging.story_body_a` 
group by 1
) x
"
red_flag4 <- query_exec(query = check_story_body_a_query,
                        project = projectId,
                        use_legacy_sql = F) %>% 
        as.integer

if(red_flag4 > 0){
        source("pwd.R")
        email <- mailR::send.mail(from = paste0(who(sender), "@gmail.com"),
                                  to = recipients,
                                  subject = error_subject_4, 
                                  body = error_body_4,
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
        stop("check out this Error_4")
        quit(save = "no")
}


################################################################
############# step (6) append story_body_b ################
## objective
# 1) load distinct stories from staging into production

## Big Query
# write_disposition = "WRITE_APPEND"
# use_legacy_sql = F

append_story_body_b_query <- "
select story_id 
, type
, ga_type
, primary_site 
, published_at
, headline
, summary 
, body 
, job_number

from `media-data-science.Jim_Story_Staging.story_body_a`
"
start6 <- Sys.time()
step6 <- insert_query_job(append_story_body_b_query, 
                          project = projectId,
                          destination_table = list(
                                  project_id = projectId,
                                  dataset_id = dataset_production,
                                  table_id = "story_body_b"),
                          # destination_table = "media-data-science:Jim_Story_Production.story_body_b",
                          write_disposition = "WRITE_APPEND",
                          use_legacy_sql = F)
wait_for(job = step6, quiet = FALSE, pause = 0.5)
end6 <- Sys.time()
step6_run_time <- paste0("step 6 'append_story_body_b_query' took ", 
                        round(difftime(end6, start6, units = 'secs'), 2),
                        " seconds")


#####################################################################
########## step (7) truncate story_body_audit_a ###################
## objectives
# 1) check and log
# 2) continue to job2 for text mining or not

## Big Query
# write_disposition = "WRITE_TRUNCATE"
# use_legacy_sql = F

truncate_story_body_audit_a_query <- "
with x
as (
    select job_number
    , distinct_story_load
    , accumulated_story_load
    from `media-data-science.Jim_Story_Audit.story_body_audit_a`
    where job_number = (select max(job_number) from `media-data-science.Jim_Story_Audit.story_body_audit_a`)
)

select sub2.job_number as job_number
, sub2.distinct_story_load as distinct_story_load
, sub2.accumulated_story_load as accumulated_story_load
, case when sub2.accumulated_story_load - sub2.previous_load = sub2.distinct_story_load then 1 else 0 end as story_load_success

from (

    select sub.job_number as job_number
    , sub.distinct_story_load as distinct_story_load 
    , sub.accumulated_story_load as accumulated_story_load
    , lag(sub.accumulated_story_load, 1, 0) over (order by sub.job_number) as previous_load  

    from ( 
        select a.job_number as job_number
        , a.distinct_story_load as distinct_story_load
        , a.distinct_story_load + x.accumulated_story_load as accumulated_story_load

        from (
            select a.job_number as job_number
            , count(a.story_id) as distinct_story_load
            from `media-data-science.Jim_Story_Staging.story_body_a` a  -- staging
            group by 1
        ) a 
        cross join x

        union all 

        select job_number
        , distinct_story_load
        , accumulated_story_load
        from `media-data-science.Jim_Story_Audit.story_body_audit_a`  
    ) sub

) sub2 

order by 1
"
start7 <- Sys.time()
step7 <- insert_query_job(truncate_story_body_audit_a_query, 
                          project = projectId,
                          destination_table = list(
                                  project_id = projectId,
                                  dataset_id = dataset_audit,
                                  table_id = "story_body_audit_a"),
                          # destination_table = "media-data-science:Jim_Story_Audit.story_body_audit_a",
                          write_disposition = "WRITE_TRUNCATE",
                          use_legacy_sql = F)
wait_for(job = step7, quiet = FALSE, pause = 0.5)
end7 <- Sys.time()
step7_run_time <- paste0("step 7 'truncate_story_body_audit_a_query' took ", 
                        round(difftime(end7, start7, units = 'secs'), 2),
                        " seconds")


################################################################
####################### run logic_E here #######################

red_flag5 <- query_exec(query = "SELECT sum(case when story_load_success = 1 then 0 else 1 end) as red_flag FROM `media-data-science.Jim_Story_Audit.story_body_audit_a`",
                        project = projectId,
                        use_legacy_sql = F) %>% 
        as.integer

if(red_flag5 > 0){
        source("pwd.R")
        email <- mailR::send.mail(from = paste0(who(sender), "@gmail.com"),
                                  to = recipients,
                                  subject = error_subject_5, 
                                  body = error_body_5,
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
        stop("check out this Error_5")
        quit(save = "no")
}

################################################################


## store run_time
run_time_end <- Sys.time()
total_run_time <- paste0("total run time of this job took approximately ", 
                        round(difftime(run_time_end, run_time_start, units = 'secs'), 2),
                        " seconds")

job_number <- query_exec(query = "select max(job_number) from [media-data-science:Jim_Story_Audit.audit_a]",
                       project = projectId,
                       use_legacy_sql = T) %>% 
        as.integer

########################################
############ write time log ############
run_time <- str_subset(ls(), 
           pattern = "^step(.)*_run_time$|total_run_time") %>%
        # print %>%
        as.list %>%
        map(., function(x){eval(rlang::sym(x))}) %>%
        # print %>%
        map(., function(x){
                str_extract_all(x, "[0-9]+\\.[0-9]+ seconds") %>%
                        str_replace_all(pattern = " seconds", replacement = "") %>%
                        as.numeric
        }) 

# Big Query automatically "sync" or align the time (from EDT run in this local machine) to UTC when storing
run_time_log <- data.frame(job_number,
                           run_time_start,
                           run_time_end,
                           step1 = run_time[[1]],
                           step2 = run_time[[2]],
                           step3 = run_time[[3]],
                           step4 = run_time[[4]],
                           step5 = run_time[[5]],
                           step6 = run_time[[6]],
                           step7 = run_time[[7]],
                           total_run_time = run_time[[8]])
                           
insert_upload_job(values = run_time_log,
                  project = projectId,
                  dataset = dataset_audit,
                  table = "run_time_log",
                  write_disposition = "WRITE_APPEND"  # MUST DO APPEND ONLY!
) %>%
        invisible


###############################
######## email success ########
log <- str_subset(ls(), 
           pattern = "^step(.)*_run_time$|total_run_time") %>%
        as.list %>%
        map(., function(x){eval(rlang::sym(x))}) %>%
        unlist %>% 
        map(., function(x){paste("-", x, sep = " ")}) %>%
        str_wrap(width = 100) %>%
        str_c(sep = "\n", collapse = " \n")

log <- str_c(success_body, log, sep = "\n\n")

source("pwd.R")
email <- mailR::send.mail(from = paste0(who(sender), "@gmail.com"),
                          to = recipients,
                          subject = success_subject, 
                          body = log,
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

# source dailyStoryBody_tm_load.R
source("dailyStoryBody_tm_load.R")