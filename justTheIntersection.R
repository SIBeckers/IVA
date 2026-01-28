#Libraries -------
library(terra)
library(sf)
library(httr2)
library(AzureStor)
library(units)
library(glue)
library(future.apply)
library(leaflet)
library(htmlwidgets)
library(leafpop)
library(leaflet.extras2)
library(matrixStats)
#Params ---------
params = NULL
params$FSmethod = "full"
params$runDate <- Sys.Date()
params$forecastDays = "03"
params$authkey = "
params$azKey = ""
params$n_workers=12
params$outdir<-"C:/Users/jbeckers/OneDrive - NRCan RNCan/Sharing/EcumeneAnalysis/"
# params$outdir<-"C:/Users/jbeckers/OneDrive - NRCan RNCan/Documents/Repos/Community_Fire/test_output/"
# Helper Funcs -----
interpolationStats <- function(x,y) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(rep(NA, 9))
  
  q <- quantile(x, probs = c(0.05, 0.25, 0.5, 0.75, 0.95), names = FALSE)
  
  c("n" = length(x),
    "Min" = min(x),
    "P05" = q[1],
    "P25" = q[2],
    "P50" = q[3],
    "Mean" = mean2(x),
    "P75" = q[4],
    "P95" = q[5],
    "Max" = max(x))
}
options(future.rng.onMisuse="ignore")


getYesterdaysFiles <- function(analysis="Ecumene"){
  list <- list.files(path="C:/Users/jbeckers/OneDrive - NRCan RNCan/Sharing/EcumeneAnalysis/",
                     pattern=paste0(analysis,".*_",format(Sys.Date()-1, "%Y%m%d"),".*_day_.*.gpkg"),
                     full.names=T) |> (\(x) x[length(x)])()
}

extract_Parallel<-function(x,outname,workers,field="OBJECTID"){
  chunks <- split(x, cut(seq_len(nrow(x)), workers, labels = FALSE))
  st=Sys.time()
  plan(multisession, workers = workers)
  results_list <- future_lapply(chunks, function(chunk,field) {
    ids<-chunk[[field]]
    r_local<-terra::rast(outname)
    foo<-terra::extract(r_local, chunk, touches = TRUE, fun = interpolationStats)
    foo[,1]=ids
    foo<-merge(chunk,foo,by.x=field,by.y="ID")
    return(foo)
  },field=field,future.seed=NULL)
  plan(sequential)
  end=Sys.time()
  result<-do.call(rbind,results_list)
  print(paste0("Time to Process: ",as.numeric(difftime(end, st, units = "secs"))))
  return(result)
}


#FireStarr Blob -----
getFireSTARR <- function(day=3){
  # outname <- list.files(pattern = paste0("firestarr_",format(params$runDate,"%Y%m%d"),"[0-9]{4}_day_0",day))
  # if (length(outname) != 1) {
  blobCont <- blob_container("https://sawipsprodca.blob.core.windows.net/firestarr", 
                             key = params$azKey)
  if (params$runDate == Sys.Date()) {
    l <-  tail(list_blobs(blobCont,dir = "firestarr",recursive=F,info="name"),n=1)
    l<- list_blobs(blobCont,dir=l,info='name',recursive=F) |> (\(x) x[grepl(format(params$runDate + (day - 1), "%Y%m%d"), x)])()
    l2<-list_blobs(blobCont,dir=l,recursive=F,info='name') |> (\(x) x[grepl("[0-9].tif",x)])()
    multidownload_blob(blobCont,l2,paste0("./",l2),max_concurrent_transfers = 20)
    # outname = strsplit(l,"/")[[1]][2]
    ##Download FireSTARR from Azure BLOB----
    # if (!file.exists(paste0('./',outname))) {
    #   download_blob(blobCont,l,dest = paste0('./',outname),use_azcopy = F,overwrite = T)
    # }
  }
  # } else {
  #   l <- list_blobs(blobCont,dir = "archive")
  #   l <- list_blobs(blobCont,dir = "current")
  #     l <- l[grepl(paste0(".*_day_0",day,".*"),l[,1]),][1,1]
  #     outname = strsplit(l,"/")[[1]][2]
  #     ##Download FireSTARR from Azure BLOB----
  #     if (!file.exists(paste0('./',outname))) {
  #       download_blob(blobCont,l,dest = paste0('./',outname),use_azcopy = F,overwrite = T)
  #     }
  # } 
  rm(blobCont,l)
  # }
  return(paste0("./",l2))
}


f<-getFireSTARR(day=as.integer(params$forecastDays))
r<-mosaic(sprc(lapply(f,FUN=function(x){project(rast(x),"epsg:3978",res=100.0,use_gdal=T,method='near')})),fun='max')
f7<-getFireSTARR(day=7)
r7<-mosaic(sprc(lapply(f7,FUN=function(x){project(rast(x),"epsg:3978",res=100.0,use_gdal=T,method='near')})),fun='max')
names(r)<-"probability"
names(r7)<-"probability"
outputstr=paste(strsplit(f[1],"/")[[1]][2],strsplit(strsplit(f[1],'/')[[1]][3],"_")[[1]][2],"day",params$forecastDays,strsplit(f[1],'/')[[1]][4],collapse='_',sep='_')



# Ecumene ---------
if (file.exists(paste0(params$outdir,"Ecumene_map.html"))){
  file.remove(paste0(params$outdir,"Ecumene_map.html"))
}
ecumene = read_sf("./ECUMENE_V3.gpkg")
ecumene_intersection<-terra::extract(r,ecumene,fun=interpolationStats,bind=T)
ecumene_intersection <- ecumene_intersection[ecumene_intersection$probability.1!=Inf,]
names(ecumene_intersection)[(ncol(ecumene_intersection)-8):ncol(ecumene_intersection)] <- c("N","Min","P05","P25","Median","Mean","P75","P95","Max")
ecumene_intersection <- ecumene_intersection[,!names(ecumene_intersection) %in% c("Latitude","Longitude")]
ecumene_intersection<-terra::extract(r7,ecumene_intersection,fun=interpolationStats,bind=T)
names(ecumene_intersection)[(ncol(ecumene_intersection)-8):ncol(ecumene_intersection)] <- c("Day7_N","Day7_Min","Day7_P05","Day7_P25","Day7_Median","Day7_Mean","Day7_P75","Day7_P95","Day7_Max")
ecumene_intersection <- st_as_sf(ecumene_intersection)


## Compare to Yesterday ----
if(nrow(ecumene_intersection)>0){
  if(length(getYesterdaysFiles(analysis="Ecumene"))>0){
    ecumene_yesterday<-read_sf(getYesterdaysFiles(analysis="Ecumene"))
    foo<-setdiff(ecumene_intersection$OBJECTID_1,ecumene_yesterday$OBJECTID_1)
    ecumene_intersection$new<-F
    ecumene_intersection$new[ecumene_intersection$OBJECTID_1 %in% foo]=T
  } else{
    ecumene_intersection$new<-T
  }
  ## Link In Evacs ----
  evacs <- st_read("https://app-geoserver2-cwfis-dev.jollycliff-a8886b94.canadacentral.azurecontainerapps.io/geoserver/restricted/ows?service=WFS&version=2.0.1&request=GetFeature&typeName=restricted%3Aevacuations_current&maxFeatures=500&outputFormat=application%2Fjson&authkey=866b073d-61e0-490b-9d9f-a50b2fa55bb0&cql_filter=active=true&outputCRS=EPSG:3978") |>
    st_transform(3978) |> st_buffer(units::set_units(2500,"m"))
  if (nrow(evacs)>0){
    foo<-st_intersection(ecumene_intersection,evacs)
    ecumene_intersection$evacuated=F
    ecumene_intersection$evacuated[ecumene_intersection$OBJECTID_1 %in% foo$OBJECTID_1] = T 
  } else{
    ecumene_intersection$evacuated=F
  }
  write_sf(ecumene_intersection,paste0(params$outdir,"Ecumene_",outputstr,'.csv'),append = F,delete_layer = T,delete_dsn=T)
  write_sf(ecumene_intersection,paste0(params$outdir,"Ecumene_",outputstr,'.gpkg'),append = F,delete_layer = T,delete_dsn=T)
  ecumene_intersection<-st_transform(ecumene_intersection,4326)
  ecumene<-st_transform(ecumene,4326)
  m <- leaflet(leafletOptions(worldCopyJump=F)) %>%
    addTiles() %>%
    addWMS(
      baseUrl = "https://app-geoserver-wips-cwfis-prod.azurewebsites.net/geoserver/firestarr/wms?",
      layerId = "FireSTARR",
      group = "FireSTARR",
      layers = "firestarr:FireSTARR",
      options = WMSTileOptions(
        version = "1.3.0",
        format = "image/png",
        authkey = params$authkey,
        TIME = params$runDate + (as.integer(params$forecastDays)-1),
        transparent = TRUE,
        noWrap=T,
        zIndex=15
      )
    ) %>%
    addWMS(
      baseUrl = "https://cwfis.cfs.nrcan.gc.ca/geoserver/wms?",
      layerId = "ActiveFires",
      group = "ActiveFires",
      layers = "public:activefires_current",
      options = WMSTileOptions(
        version = "1.3.0",
        format = "image/png",
        transparent = TRUE,
        noWrap=T,
        zIndex=20
      )
    ) %>%
    addPolygons(
      data = ecumene,
      label = ~ecumene$EcuName,
      popup = ~popupTable(ecumene, row.numbers=F),
      group = "Ecumene",
      fillColor="lightgrey",
      fillOpacity=0.5,
      color="lightgrey",
      opacity=0.5
    ) %>%
    addPolygons(
      data = ecumene_intersection,
      label = ~ecumene_intersection$EcuName,
      popup = ~popupTable(ecumene_intersection, row.numbers=F),
      group = "Ecumene",
      fillColor="purple",
      fillOpacity=0.6,
      color="purple",
      opacity=1
    )
  if(sum(ecumene_intersection$new)>0){
    m<-m %>%
      addPolygons(
        data = ecumene_intersection[ecumene_intersection$new==T,],
        label = ~ecumene_intersection[ecumene_intersection$new==T,]$EcuName,
        popup = ~popupTable(ecumene_intersection[ecumene_intersection$new==T,], row.numbers=F),
        group = "Ecumene",
        fillColor="orange",
        fillOpacity=0.8,
        color="orange",
        opacity=1
      ) 
  }
  m <- m %>%
    addLayersControl(
      baseGroups = c("basegroup"),
      overlayGroups = c("FireSTARR","ActiveFires","Ecumene"),
      position="topright"
    ) %>%
    hideGroup("FireSTARR") %>%
    hideGroup("ActiveFires")
  saveWidget(m, file = paste0(params$outdir,"Ecumene_map.html"),selfcontained = T,title="Ecumene Intersection")
} else {
  write_sf(ecumene_intersection,paste0(params$outdir,"Ecumene_",outputstr,'.csv'),append = F,delete_layer = T,delete_dsn=T)
  write_sf(ecumene_intersection,paste0(params$outdir,"Ecumene_",outputstr,'.gpkg'),append = F,delete_layer = T,delete_dsn=T)
}
rm(ecumene,ecumene_intersection)
# Highways -------
if (file.exists(paste0(params$outdir,"Highways_map.html"))){
  file.remove(paste0(params$outdir,"Highway_map.html"))
}
highways<-read_sf("./highways_v2.gpkg")
highways_inter<-na.omit(extract_Parallel(highways,terra::sources(r),workers=params$n_workers,field="ID"))
names(highways_inter)[7:15] <- c("N","Min","P05","P25","Median","Mean","P75","P95","Max")
highways_inter<-na.omit(st_as_sf(terra::extract(r7,highways_inter,bind=T,touches=T,fun=interpolationStats)))
names(highways_inter)[(16):24] <- c("Day7_N","Day7_Min","Day7_P05","Day7_P25","Day7_Median","Day7_Mean","Day7_P75","Day7_P95","Day7_Max")
if(nrow(highways_inter)>0){
  if(length(getYesterdaysFiles(analysis="Highways"))>0){
    highways_yesterday<-read_sf(getYesterdaysFiles(analysis="Highways"))
    names(highways_yesterday)[1]<-"ID"
    foo<-setdiff(highways_inter$ID,highways_yesterday$ID)
    highways_inter$new<-F
    highways_inter$new[highways_inter$ID %in% foo]=T
  } else {
    highways_inter$new<-T
  }
  write_sf(highways_inter,paste0(params$outdir,"Highways_",outputstr,".csv"),append = F,delete_layer = T,delete_dsn=T)
  write_sf(highways_inter,paste0(params$outdir,"Highways_",outputstr,".gpkg"),append = F,delete_layer = T,delete_dsn=T)
  highways_inter<-st_transform(highways_inter,4326)
  highways<-st_transform(highways,4326)
  m <- leaflet(leafletOptions(worldCopyJump=F)) %>%
    addTiles() %>%
    addWMS(
      baseUrl = "https://app-geoserver-wips-cwfis-prod.azurewebsites.net/geoserver/firestarr/wms?",
      layerId = "FireSTARR",
      group = "FireSTARR",
      layers = "firestarr:FireSTARR",
      options = WMSTileOptions(
        version = "1.3.0",
        format = "image/png",
        authkey = params$authkey,
        TIME = params$runDate + (as.integer(params$forecastDays)-1),
        transparent = TRUE,
        noWrap=T,
        zIndex=9
      )
    ) %>%
    addWMS(
      baseUrl = "https://cwfis.cfs.nrcan.gc.ca/geoserver/wms?",
      layerId = "ActiveFires",
      group = "ActiveFires",
      layers = "public:activefires_current",
      options = WMSTileOptions(
        version = "1.3.0",
        format = "image/png",
        transparent = TRUE,
        noWrap=T,
        zIndex=10
      )
    ) %>%
    addPolylines(
      data = highways,
      group = "Highways",
      fillColor="darkgrey",
      fillOpacity=0.8,
      color="darkgrey",
      opacity=1,
      weight=1
    ) %>% 
    addPolylines(
      data = highways_inter,
      label = ~paste0("Route: ",highways_inter$rtenum1),
      popup = ~popupTable(highways_inter, row.numbers=F),
      group = "Highways",
      fillColor="purple",
      fillOpacity=0.8,
      color="purple",
      opacity=1
    )
  if (sum(highways_inter$new)>0){
    m <- m %>%
      addPolylines(
        data = highways_inter[highways_inter$new==T,],
        label = ~paste0("Route: ",highways_inter[highways_inter$new==T,]$rtenum1),
        popup = ~popupTable(highways_inter[highways_inter$new==T,], row.numbers=F),
        group = "Highways",
        fillColor="orange",
        fillOpacity=0.8,
        color="orange",
        opacity=1
      )
  }
  m <- m %>%
    addLayersControl(
      baseGroups = c("basegroup"),
      overlayGroups = c("FireSTARR","ActiveFires","Highways"),
      position="topright"
    ) %>%
    hideGroup("FireSTARR") %>%
    hideGroup("ActiveFires")
  saveWidget(m, file = paste0(params$outdir,"Highways_map.html"),selfcontained = T,title="Highways Intersection")
} else {
  write_sf(highways_inter,paste0(params$outdir,"Highways_",outputstr,".csv"),append = F,delete_layer = T,delete_dsn=T)
  write_sf(highways_inter,paste0(params$outdir,"Highways_",outputstr,".gpkg"),append = F,delete_layer = T,delete_dsn=T)
}
rm(highways,highways_inter)
# Rail -----
if (file.exists(paste0(params$outdir,"Rail_map.html"))){
  file.remove(paste0(params$outdir,"Rail_map.html"))
}
rail<-read_sf("./railways_v2.gpkg")
rail_inter<-na.omit(extract_Parallel(rail,sources(r),workers=params$n_workers,field="ID"))
names(rail_inter)[8:16] <- c("N","Min","P05","P25","Median","Mean","P75","P95","Max")
rail_inter<-na.omit(st_as_sf(terra::extract(r7,rail_inter,bind=T,touches=T,fun=interpolationStats)))
names(rail_inter)[17:25] <- c("Day7_N","Day7_Min","Day7_P05","Day7_P25","Day7_Median","Day7_Mean","Day7_P75","Day7_P95","Day7_Max")
if(nrow(rail_inter)>0){
  if(length(getYesterdaysFiles(analysis="Railways"))>0){
    rail_yesterday<-read_sf(getYesterdaysFiles(analysis="Railways"))
    names(rail_yesterday)[1]<-"ID"
    foo<-setdiff(rail_inter$ID,rail_yesterday$ID)
    rail_inter$new<-F
    rail_inter$new[rail_inter$ID %in% foo]=T
  } else {
    rail_inter$new<-T
  }
  write_sf(rail_inter,paste0(params$outdir,"Railways_",outputstr,".csv"),append = F,delete_layer = T,delete_dsn=T)
  write_sf(rail_inter,paste0(params$outdir,"Railways_",outputstr,".gpkg"),append = F,delete_layer = T,delete_dsn=T)
  rail_inter<-st_transform(rail_inter,4326)
  rail<-st_transform(rail,4326)
  m <- leaflet(leafletOptions(worldCopyJump=F)) %>%
    addTiles() %>%
    addWMS(
      baseUrl = "https://app-geoserver-wips-cwfis-prod.azurewebsites.net/geoserver/firestarr/wms?",
      layerId = "FireSTARR",
      group = "FireSTARR",
      layers = "firestarr:FireSTARR",
      options = WMSTileOptions(
        version = "1.3.0",
        format = "image/png",
        authkey = params$authkey,
        TIME = params$runDate + (as.integer(params$forecastDays)-1),
        transparent = TRUE,
        noWrap=T,
        zIndex=9
      )
    ) %>%
    addWMS(
      baseUrl = "https://cwfis.cfs.nrcan.gc.ca/geoserver/wms?",
      layerId = "ActiveFires",
      group = "ActiveFires",
      layers = "public:activefires_current",
      options = WMSTileOptions(
        version = "1.3.0",
        format = "image/png",
        transparent = TRUE,
        noWrap=T,
        zIndex=10
      )
    ) %>%
    addPolylines(
      data = rail,
      group = "Rail",
      fillColor="darkgrey",
      fillOpacity=0.8,
      color="darkgrey",
      opacity=1,
      weight=1
    ) %>% 
    addPolylines(
      data = rail_inter,
      label = ~paste0("Rail Subdivision: ",rail_inter$subnam1_en),
      popup = ~popupTable(rail_inter, row.numbers=F),
      group = "Rail",
      fillColor="purple",
      fillOpacity=0.8,
      color="purple",
      opacity=1
    ) 
  if (sum(rail_inter$new)>0){
    m <- m %>%
      addPolylines(
        data = rail_inter[rail_inter$new==T,],
        label = ~paste0("Rail Subdivision: ",rail_inter[rail_inter$new==T,]$subnam1_en),
        popup = ~popupTable(rail_inter[rail_inter$new==T,], row.numbers=F),
        group = "Rail",
        fillColor="orange",
        fillOpacity=0.8,
        color="orange",
        opacity=1
      ) 
  }
  m <- m %>%
    addLayersControl(
      baseGroups = c("basegroup"),
      overlayGroups = c("FireSTARR","ActiveFires","Rail"),
      position="topright"
    ) %>%
    hideGroup("FireSTARR") %>%
    hideGroup("ActiveFires")
  saveWidget(m, file = paste0(params$outdir,"Rail_map.html"),selfcontained = T,title="Railways Intersection")
} else{ 
  write_sf(rail_inter,paste0(params$outdir,"Railways_",outputstr,".csv"),append = F,delete_layer = T,delete_dsn=T)
  write_sf(rail_inter,paste0(params$outdir,"Railways_",outputstr,".gpkg"),append = F,delete_layer = T,delete_dsn=T)
}
rm(rail,rail_inter)

# Facilities -----
#Mills, mines, etc.
if (file.exists(paste0(params$outdir,"Facilities_map.html"))){
  file.remove(paste0(params$outdir,"Facilities_map.html"))
}
facilities<-read_sf("./facilities.gpkg",layer="facilities")
facilities_inter<-na.omit(st_as_sf(terra::extract(r,facilities,bind=T,touches=T,fun=interpolationStats)))
facilities_inter<-na.omit(st_as_sf(terra::extract(r7,facilities_inter,bind=T,touches=T,fun=interpolationStats)))
names(facilities_inter)[6:14] <- c("N","Min","P05","P25","Median","Mean","P75","P95","Max")
names(facilities_inter)[15:23] <- c("Day7_N","Day7_Min","Day7_P05","Day7_P25","Day7_Median","Day7_Mean","Day7_P75","Day7_P95","Day7_Max")
if(nrow(facilities_inter)>0){
  if(length(getYesterdaysFiles(analysis="Facilities"))>0){
    facilities_yesterday<-read_sf(getYesterdaysFiles(analysis="Facilities"))
    foo<-setdiff(facilities_inter$ID,facilities_yesterday$ID)
    facilities_inter$new<-F
    facilities_inter$new[facilities_inter$ID %in% foo]=T
  } else{
    facilities_inter$new<-T
  }
  write_sf(facilities_inter,paste0(params$outdir,"Facilities_",outputstr,".csv"),append = F,delete_layer = T,delete_dsn=T)
  write_sf(facilities_inter,paste0(params$outdir,"Facilities_",outputstr,".gpkg"),append = F,delete_layer = T,delete_dsn=T)
  facilities_inter<-st_transform(facilities_inter,4326)
  facilities<-st_transform(facilities,4326)
  
  m <- leaflet(leafletOptions(worldCopyJump=F)) %>%
    addTiles() %>%
    addWMS(
      baseUrl = "https://app-geoserver-wips-cwfis-prod.azurewebsites.net/geoserver/firestarr/wms?",
      layerId = "FireSTARR",
      group = "FireSTARR",
      layers = "firestarr:FireSTARR",
      options = WMSTileOptions(
        version = "1.3.0",
        format = "image/png",
        authkey = params$authkey,
        TIME = params$runDate + (as.integer(params$forecastDays)-1),
        transparent = TRUE,
        noWrap=T,
        zIndex=9
      )
    ) %>%
    addWMS(
      baseUrl = "https://cwfis.cfs.nrcan.gc.ca/geoserver/wms?",
      layerId = "ActiveFires",
      group = "ActiveFires",
      layers = "public:activefires_current",
      options = WMSTileOptions(
        version = "1.3.0",
        format = "image/png",
        transparent = TRUE,
        noWrap=T,
        zIndex=10
      )
    ) %>%
    addPolygons(
      data = facilities,
      group = "Facilities",
      label = ~paste0("Facility: ",facilities$Name),
      fillColor="darkgrey",
      fillOpacity=0.8,
      color="darkgrey",
      opacity=1,
      weight=2
    ) %>% 
    addPolygons(
      data = facilities_inter,
      label = ~paste0("Facility: ",facilities_inter$Name),
      popup = ~popupTable(facilities_inter, row.numbers=F),
      group = "Facilities",
      fillColor="purple",
      fillOpacity=0.8,
      color="purple",
      opacity=1
    ) 
  if (sum(facilities_inter$new)>0){
    m<- m %>%
      addPolygons(
        data = facilities_inter[facilities_inter$new==T,],
        label = ~paste0("Facility: ",facilities_inter[facilities_inter$new==T,]$Name),
        popup = ~popupTable(facilities_inter[facilities_inter$new==T,], row.numbers=F),
        group = "Facilities",
        fillColor="orange",
        fillOpacity=0.8,
        color="orange",
        opacity=1
      )
  }
  m <- m %>%
    addLayersControl(
      baseGroups = c("basegroup"),
      overlayGroups = c("FireSTARR","ActiveFires","Facilities"),
      position="topright"
    ) %>%
    hideGroup("FireSTARR") %>%
    hideGroup("ActiveFires")
  saveWidget(m, file = paste0(params$outdir,"Facilities_map.html"),selfcontained = T,title="Facilities Intersection")
} else {
  write_sf(facilities_inter,paste0(params$outdir,"Facilities_",outputstr,".csv"),append = F,delete_layer = T,delete_dsn=T)
  write_sf(facilities_inter,paste0(params$outdir,"Facilities_",outputstr,".gpkg"),append = F,delete_layer = T,delete_dsn=T)
} 
rm(facilities,facilities_inter)

# FNs -----
# FN's 
if (file.exists(paste0(params$outdir,"FirstNations_map.html"))){
  file.remove(paste0(params$outdir,"FirstNations_map.html"))
}
fns<-read_sf("./FirstNations.gpkg")
fns_inter<-na.omit(terra::extract(r,fns,bind=T,touches=T,fun=interpolationStats),field=c("probability",paste0("probability.",1:8)))
fns_inter<-st_as_sf(terra::extract(r7,fns_inter,bind=T,touches=T,fun=interpolationStats))

names(fns_inter)[11:19] <- c("N","Min","P05","P25","Median","Mean","P75","P95","Max")
names(fns_inter)[20:28] <- c("Day7_N","Day7_Min","Day7_P05","Day7_P25","Day7_Median","Day7_Mean","Day7_P75","Day7_P95","Day7_Max")
if(nrow(fns_inter)>0){
  if(length(getYesterdaysFiles(analysis="FirstNations"))>0){
    fns_yesterday<-read_sf(getYesterdaysFiles(analysis="FirstNations"))
    foo<-setdiff(fns_inter$ID,fns_yesterday$ID)
    fns_inter$new<-F
    fns_inter$new[fns_inter$ID %in% foo]=T
  } else{
    fns_inter$new<-T
  }
  write_sf(fns_inter,paste0(params$outdir,"FirstNations_",outputstr,".csv"),append = F,delete_layer = T,delete_dsn=T)
  write_sf(fns_inter,paste0(params$outdir,"FirstNations_",outputstr,".gpkg"),append = F,delete_layer = T,delete_dsn=T)
  fns_inter<-st_transform(fns_inter,4326)
  fns<-st_transform(fns,4326)
  
  m <- leaflet(leafletOptions(worldCopyJump=F)) %>%
    addTiles() %>%
    addWMS(
      baseUrl = "https://app-geoserver-wips-cwfis-prod.azurewebsites.net/geoserver/firestarr/wms?",
      layerId = "FireSTARR",
      group = "FireSTARR",
      layers = "firestarr:FireSTARR",
      options = WMSTileOptions(
        version = "1.3.0",
        format = "image/png",
        authkey = params$authkey,
        TIME = params$runDate + (as.integer(params$forecastDays)-1),
        transparent = TRUE,
        noWrap=T,
        zIndex=9
      )
    ) %>%
    addWMS(
      baseUrl = "https://cwfis.cfs.nrcan.gc.ca/geoserver/wms?",
      layerId = "ActiveFires",
      group = "ActiveFires",
      layers = "public:activefires_current",
      options = WMSTileOptions(
        version = "1.3.0",
        format = "image/png",
        transparent = TRUE,
        noWrap=T,
        zIndex=10
      )
    ) %>%
    addPolygons(
      data = fns,
      group = "First Nation",
      label = ~paste0("Band Name: ",fns$BAND_NAME, " First Nation: ",fns$FIRST_NATIONS),
      fillColor="darkgrey",
      fillOpacity=0.8,
      color="darkgrey",
      opacity=1,
      weight=2
    ) %>% 
    addPolygons(
      data = fns_inter,
      label = ~paste0("Band Name: ",fns_inter$BAND_NAME, " First Nation: ",fns_inter$FIRST_NATIONS),
      popup = ~popupTable(fns_inter, row.numbers=F),
      group = "fns",
      fillColor="purple",
      fillOpacity=0.8,
      color="purple",
      opacity=1
    ) 
  if (sum(fns_inter$new)>0){
    m<- m %>%
      addPolygons(
        data = fns_inter[fns_inter$new==T,],
        label = ~paste0("Band Name:: ",fns_inter[fns_inter$new==T,]$BAND_NAME," First Nation: ",fns_inter[fns_inter$new==T,]$FIRST_NATIONS),
        popup = ~popupTable(fns_inter[fns_inter$new==T,], row.numbers=F),
        group = "First Nation",
        fillColor="orange",
        fillOpacity=0.8,
        color="orange",
        opacity=1
      )
  }
  m <- m %>%
    addLayersControl(
      baseGroups = c("basegroup"),
      overlayGroups = c("FireSTARR","ActiveFires","First Nation"),
      position="topright"
    ) %>%
    hideGroup("FireSTARR") %>%
    hideGroup("ActiveFires")
  saveWidget(m, file = paste0(params$outdir,"FirstNations_map.html"),selfcontained = T,title="First Nations Intersection")
} else {
  write_sf(facilities_inter,paste0(params$outdir,"FirstNations_",outputstr,".csv"),append = F,delete_layer = T,delete_dsn=T)
  write_sf(facilities_inter,paste0(params$outdir,"FirstNations_",outputstr,".gpkg"),append = F,delete_layer = T,delete_dsn=T)
} 


### Get rid of firestarr layer-----
file.remove(f,f7)
# unlink(paste(strsplit(f7[1],"/")[[1]][2:4],collapse='/'),recursive=T,force=T)
unlink(paste(strsplit(f[1],"/")[[1]][2],collapse='/'),recursive=T,force=T)
