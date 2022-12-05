#install.packages("httr2")
# install.packages("jsonlite")

require(httr)
require(jsonlite)
require(fs)
require(sys)
library(rio)
library(data.table)



base <- "http://192.168.1.206:8000/api4opt/"
access <- "JWT eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNjcxMjEwNzIzLCJqdGkiOiJkMDM5YzVjNWNjNzc0ZDA4YWQ5YTg3MGM3ZTUwZWViZSIsInVzZXJfaWQiOjF9.dcGEJmwDW3BTHzwXJaQnFB83I2h7jPfUCIGDGXtENEU"

lsBase <- function() {
    url <- paste(base, "bases/", sep="")
    resp <- httr::GET(url, add_headers('Authorization'= access, 'Content-Type' = 'application/x-www-form-urlencoded'))
    if (resp$status_code == 200) {
        results <- content(resp, as = 'parsed')
        print(paste('There are', results$count, 'base scenarios'))
    }

}

getBase <- function( data) {
    data['title'] = title
    data['counties'] = counties 
    
    url <- paste(url_base, 'bases/', sep="")
    response <- httr::GET(url, add_headers('Authorization'= access, 'Content-Type' = 'application/x-www-form-urlencoded'))
    cid <- -1
    if (response$status_code == 200) {
        results <- content(resp, as = 'parsed')
        print(paste('There are', results$count, 'base scenarios'))
        cid <- results$id
    }
    else{
        print(paste('The base scenario was not sent for execution. Please check the following status code: ', response$status_code))
    }

    return cid
    
}

getFiles <- function(path, cid, typeFile) {
    url  <- paste(base, typeFile, cid, sep="/")
    fs::dir_create(path)
    while(TRUE)
    {
        response <- httr::GET(url, add_headers('Authorization'= access, 'Content-Type' = 'application/x-www-form-urlencoded'))
        if (response$status_code == 200) {
            results <- content(resp, as = 'parsed')
            if (results$status == 'C') {
                file_list  <- ''
                for (file_data in results$files) { # donwload the files
                    urlFile <- fileData[paste(typeFile, '_file')]
                    filename <- os.path.basename(url_file)
                    fullFilename  <- paste(path, filename, sep='/')
                    if (endsWith(fullFilename, "zip")) {
                        download.file(url_file, fullFilename, method="curl")
                        cat(paste("unziping:", fullFilename, sep=" "))
                        #https://stackoverflow.com/questions/3053833/using-r-to-download-zipped-data-file-extract-and-import-data 
                        unzip(zipfile=fullFilename, exdir=path, overwrite=TRUE)
                    }

                    #if (endsWith(full_filename, 'zip') {
                    #    with ZipFile(full_filename) as z_object:
                    #        z_object.extractall(path=path)
                    #}
                }

                print(paste('Files downloaded: ', file_list))
                break
            } else if (results$status == 'P') { #  not finished yet, lets wait
                seconds <- 30
                print(paste('Not finished yet. Sleeping for',seconds, 'seconds'))
                time.sleep(seconds)
            } else if (results$status == 'F') { #  not finished yet, lets wait
                print('Error, the evaluation failed')
                break
            }
        } else {
            print(paste('Error:', response$status_code, ))
            break 
        }
    }
}

getExecution <- function(title, base, popsize, ngen, cc_start_gen, eps_reduction, eps_iters) {
    data  <- '{
        "title" : title,
        "base" : base,
        "algorithm" : 'N',
        "parameters" : [popsize, ngen, cc_start_gen, eps_reduction, eps_iters],
        "notify" : False 
    }'
    
    
    url  <- paste(url_base, 'executions/', sep="/")

    response <- httr::POST(url, add_headers('Authorization'= access, 'Content-Type' = 'application/x-www-form-urlencoded'))
    cid  <- -1
    
    if (response$status_code == 200) {
        results <- content(response, as = 'parsed')
        print(paste('There are', results$count, 'base scenarios'))
        cid  <- results$id
        print(paste(title, 'with ID', cid, 'successfully sent to execution', sep=" "))
    } else {
        print(paste('The execution was not sent. Please check the following status code: ', response.status_code))
        exit()
    }

    return cid            
}

copyParts <- function(path) {
    cost  <- []
    load  <- []
    idx  <- []
    for (i in 0:100) {
        cost_output_file  <- paste(path, "/", i, '_output_t.csv', sep='')
        load_output_file  <- paste(path, "/", i, '_report_loads.csv', sep='')
    
        file_exists  <- os.path.exists(cost_output_file)
        
        if (file_exists) {
            csvFile  <- read.csv(cost_output_file)
            cost.append(csvFile['Cost'].sum())
            loadFile  <- pandas.read_csv(load_output_file)
            load.append(loadFile['NLoadEos'].sum())
            idx.append(i)
        }
    }
    
            
    ret = {}

    for (i in range(len(idx))) {
        ret[idx[i]] <- [cost[i], load[i]]
    }

    return ret
}
    
doe <- function(path) { 

    doeFilename  <- paste(path, doe.settings, sep='/')
    doeFile <-  jsonlite::fromJSON(doeFilename)

    params  <- list() 
    nexec  <- 1

    for (param in c('popsize', 'ngen', 'cc_start_gen', 'eps_reduction', 'eps_iters')) {
        if  (param in doe.keys()) {
            params <- append(params, list(doe$param))
        } else {
            print('ERROR: no {} in data'.format(pararm))
            exit()
        } if ('nexec' in doe.keys()) {
            nexec = doe['nexec']
        }
    }
    
    for (key, value in doe['geographies'].items()) {
        if ('base_id' in value.keys()) {
            base_id = value['base_id']
        } else {
            base_id  <- get_base(key, value['geo_ids'], doe['base_scenario'])
            doe['geographies'][key]['base_id']  <- base_id
            get_files('{}/{}/base'.format(path,base_id), base_id, 'base')
            jsonlite::write_json(doe , doeFilename)
        }

        executions  <- {}
        if ('executions' in value.keys()) {
            executions  <- value['executions'] 
        }

        idx_missed  <- list(range(nexec)) 

        for (idx, _ in executions.items()) {
            pf_path  <- paste(path, base_id, 'executions', idx, 'results/pareto_front.out', sep='/')
            if (os.path.exists(pf_path)) {
                idx_missed.remove(idx)
            }
        }


        for (idx in idx_missed) {
            t0  <- Sys.time() 
            exec_path  <- paste(path, base_ind, 'executions', idx, sep='/')
            exec_id  <- get_execution(title=key, base=base_id, **params)
            get_files(exec_path, exec_id, 'execution')
            t1  <- Sys.time() 
            fx  <- copy_parts('{}/results'.format(exec_path))
            lapseTime  <- difftime(t0, t1, units= 'secs')
            executions[idx] <- list(exec_num=idx, exec_id=exec_id, time_s=lapseTime, fx=fx)
            doe['geographies'][key]['executions']  <- executions 
            jsonlite::write_json(doe , doeFilename)
        }
    }
}

doe.settings  <- '{ 
    "base_scenario": {
      "atm_dep_data_set": 38,
      "back_out_scenario": 6611,
      "base_condition": 256,
      "base_load": 6,
      "cost_profile": 8,
      "climate_change_data_set": 59,
      "historical_crop_need_scenario": 6608,
      "point_source_data_set": 36,
      "scenario_type": 2,
      "soil_p_data_set": 31,
      "source_data_revision": 8
    },
    "popsize": 20,
    "ngen": 100,
    "cc_start_gen": 96,
    "eps_reduction": 0.2,
    "eps_iters": 20,
    "nexec": 11,
    "geographies":{
        "Tucker": {"geo_ids":[416]}
    }
    
}'

doe.settings.json <-  jsonlite::fromJSON(doe.settings)

fs::dir_create("hola")

jsonlite::write_json(doe.settings.json, "test.json")


getBase(doe.settings.json,"como", "estas")

lsBase()
