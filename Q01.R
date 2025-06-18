library("ROracle")
library("stringr")

drv <- Oracle()
con <- dbConnect(drv, username = Sys.getenv("userid"), password = Sys.getenv("pwd"), dbname = "emerald.ksh.hu")

Q01 <- data.frame(matrix(NA, nrow = length(CHANGED_ALL_M003), ncol = 4))

Q01[, 1] <- "Q01"
Q01[, 2] <- paste("Q01", substr(Sys.Date(), 4, 4), substr(Sys.Date(), 6, 7), substr(Sys.Date(), 9, 10), "15302724", sep = "")
Q01[, 3] <- c(1:nrow(Q01))
  
View(Q01)

datum <- paste(substr(Sys.Date(), 1, 4), substr(Sys.Date(), 6, 7), substr(Sys.Date(), 9, 10), sep = "")

for(row in 1:length(CHANGED_ALL_M003)){
  
  KSHTORZS <- str_pad(row, width = 7, pad = "0")
  
  res <- dbSendQuery(con, paste("select M003, M0491, TO_CHAR(M0491_H, 'YYYYMMDD') M0491_H, 
                                          M005_SZH, TO_CHAR(M005_SZH_H, 'YYYYMMDD') M005_SZH_H, nev, 
                                          TO_CHAR(nev_h, 'YYYYMMDD') nev_h, rnev, TO_CHAR(rnev_h, 'YYYYMMDD') RNEV_H, 
                                          M054_SZH, TELNEV_SZH, UTCA_SZH, TO_CHAR(szekhely_h, 'YYYYMMDD') SZEKHELY_H, 
                                          M054_LEV, telnev_lev, UTCA_LEV, decode(levelezesi_h, null, TO_CHAR(levelezesi_r, 'YYYYMMDD'), TO_CHAR(levelezesi_h, 'YYYYMMDD')) LEVELEZESI_R, 
                                          M054_PF_LEV, telnev_PF_LEV, PFIOK_LEV, to_char(lev_pf_r, 'YYYYMMDD'), 
                                          M040, to_char(M040K, 'YYYYMMDD'), M025, to_char(letszam_h, 'YYYYMMDD'), 
                                          M026, to_char(arbev_h, 'YYYYMMDD') ARBEV_H, M009_SZH, to_char(alakdat, 'YYYYMMDD') ALAKDAT,
                                          M0781, to_char(M0781_H, 'YYYYMMDD'), M058_J, to_char(M0581_H, 'YYYYMMDD'), 
                                          decode(MP65, 'S9900', null, MP65) MP65, to_char(MP65_H,'YYYYMMDD') MP65_H, 
                                          to_char(UELESZT, 'YYYYMMDD') UELESZT, to_char(M003_R, 'YYYYMMDD') M003_R, 
                                          to_char(DATUM, 'YYYYMMDD') DATUM, M0581_J M0581, to_char(M0583_H, 'YYYYMMDD') M0581_H, 
                                          cegv, to_char(cegv_h, 'YYYYMMDD') CEGV_H, nvl(MVB39, '0') MVB39, nvl(to_char(MVB39_H, 'YYYYMMDD'), 
                                          case when to_char(alakdat, 'YYYY') < '2016' then '20160101' else to_char(alakdat, 'YYYYMMDD') end) MVB39_H, 
                                          null ORSZ, LETSZAM, ARBEV, M0783, to_char(M0783_H, 'YYYYMMDD') M0783_H, M0583, to_char(M0583_H, 'YYYYMMDD') M0583_H, 0 TEAOR25MILYEN, 
                                          null EELERHETOSEG_10V11, null EELERHETOSEG_10V11_HATALY, null EELERHETOSEG_60, null EELERHETOSEG_60_HATALY
                                          from VB.F003 where M003 = '", CHANGED_ALL_M003[row], "'"))
  VALUES <- fetch(res)
  dbClearResult(res)
  
  VALUES[is.na(VALUES)] <- ""
  
  M005_SZH <- VALUES[1, "M005_SZH"]
  M025 <- VALUES[1, "M025"]
  M0781 <- VALUES[1, "M0781"]
  M058_J <- VALUES[1, "M058_J"]
  CEGV <- VALUES[1, "CEGV"]
  M009_SZH <- VALUES[1, "M009_SZH"]
  
  if(VALUES[1, "MP65"] == ""){
    
    MP65_H <- NULL
    
  }else{
    
    MP65_H <- VALUES[1, "MP65_H"]
    
  }
    
  PFIOK_LEV <- VALUES[1, "PFIOK_LEV"]
  
  
  if(nchar(VALUES[1, "ARBEV_H"]) == 0){
    
    ARBEV_H <- VALUES[1, "ALAKDAT"]
    
  }else{
    
    ARBEV_H <- VALUES[1, "ARBEV_H"]
    
  }
  
  
  res <- dbSendQuery(con, paste("select M009CDV from VT.F009_AKT where M009 = '", M009_SZH, "'", sep = ""))
  M009_SZH_CDV <- fetch(res)
  dbClearResult(res)
  M009_SZH_CDV_TOGETHER <- paste(M009_SZH, M009_SZH_CDV, sep = "")
  
  res <- dbSendQuery(con, paste("select ORSZ from (select * from VB_CEG.VB_APEH_CIM where M003 = '", CHANGED_ALL_M003[row], "'  order by DATUM_R desc) where rownum < 2", sep = ""))
  ORSZ <- fetch(res)
  dbClearResult(res)
  
  if(nrow(ORSZ) == 0 || is.na(ORSZ) == TRUE){
    
    if(VALUES[1, "ORSZ"] == "" & as.numeric(VALUES[1, "M005_SZH"]) < 21){ 
      
      ORSZ <- "HU"
      cat(paste("A", VALUES[1, "M003"], "törzsszám országkódja üresről HU lett, mert a megyekód (M005_SZH = ", VALUES[1, "M005_SZH"], ") < 21.", "\n", sep = " "))
      
    }
    
    if(VALUES[1, "ORSZ"] == "" & as.numeric(VALUES[1, "M005_SZH"]) > 20){ 
      
      ORSZ <- "Z8"
      cat(paste("A", VALUES[1, "M003"], "törzsszám országkódja üresről Z8 lett, mert a megyekód (M005_SZH = ", VALUES[1, "M005_SZH"], ") > 20.", "\n", sep = " "))
      
    }
    
      
  }else{
    
    if(ORSZ == "HU" & as.numeric(VALUES[1, "M005_SZH"]) > 20){ 
      
      ORSZ <- "Z8"
      cat(paste("A", VALUES[1, "M003"], "törzsszám országkódja HU-ról Z8 lett, mert a megyekód (M005_SZH = ", VALUES[1, "M005_SZH"], ") > 20).", "\n", sep = " "))
      
    }
    
    if(ORSZ == "XX"){ 
      
      ORSZ <- "Z8"
      cat(paste("A", VALUES[1, "M003"], "törzsszám országkódja XX-ről Z8 lett, mert XX értéket az MNB nem tud fogadni.", "\n", sep = " "))
      
    }
    
  }
  
  res <- dbSendQuery(con, paste("select EELERHETOSEG, TO_CHAR(HATALY, 'YYYYMMDD') HATALY from (select EELERHETOSEG, HATALY from VB.F003_EELERHETOSEG where M003 = '", CHANGED_ALL_M003[row], "' and MVB42 in ('10', '11') order by M003, HATALY desc, MVB42, DATUM_R desc offset 0 row fetch first 1 rows only)", sep = ""))
  EELERHETOSEG_1 <- fetch(res)
  dbClearResult(res)
  
  if(nrow(EELERHETOSEG_1) == 0){
    
    EELERHETOSEG_10V11 <- ""
    EELERHETOSEG_10V11_HATALY <- ""
    
  }else{
    
    EELERHETOSEG_10V11 <- EELERHETOSEG_1[1, "EELERHETOSEG"]
    EELERHETOSEG_10V11_HATALY <- EELERHETOSEG_1[1, "HATALY"]
    
    if(VALUES[1, "ALAKDAT"] > EELERHETOSEG_10V11_HATALY){
      
      EELERHETOSEG_10V11_HATALY <- VALUES[1, "ALAKDAT"]
      cat(paste("A", VALUES[1, "M003"], "törzsszám e-mail címének hatálya (", EELERHETOSEG_1[1, "HATALY"], ") az alakulás dátumára változott:", VALUES[1, "ALAKDAT"], "\n", sep = " "))
      
    }
    
  }
  
  res <- dbSendQuery(con, paste("select replace(EELERHETOSEG, 'hivatali', 'ceg') EELERHETOSEG, TO_CHAR(HATALY, 'YYYYMMDD') HATALY from (select EELERHETOSEG, HATALY from VB.F003_EELERHETOSEG where M003 = '", CHANGED_ALL_M003[row], "' and MVB42 = '60')", sep = ""))
  EELERHETOSEG_2 <- fetch(res)
  dbClearResult(res)
  
  if(nrow(EELERHETOSEG_2) == 0){
    
    EELERHETOSEG_60 <- ""
    EELERHETOSEG_60_HATALY <- ""
    
  }else{
    
    EELERHETOSEG_60 <- EELERHETOSEG_2[1, "EELERHETOSEG"]
    EELERHETOSEG_60_HATALY <- EELERHETOSEG_2[1, "HATALY"]
    
    if(VALUES[1, "ALAKDAT"] > EELERHETOSEG_60_HATALY){
      
      EELERHETOSEG_60_HATALY <- VALUES[1, "ALAKDAT"]
      cat(paste("A", VALUES[1, "M003"], "törzsszám cégkapujának hatálya (", EELERHETOSEG_2[1, "HATALY"], ") az alakulás dátumára változott:", VALUES[1, "ALAKDAT"], "\n", sep = " "))
      
      
    }
    
  }
  
  if(VALUES[1, "LETSZAM"] == ""){
    
    LETSZAM <- 'N/A'
    
  }else{
    
    LETSZAM <- VALUES[1, "LETSZAM"]
    
  } 
  
  if(VALUES[1, "ARBEV"] == ""){
    
    ARBEV <- 'N/A'
    
  }else{
    
    ARBEV <- VALUES[1, "ARBEV"]
    
  } 
  
  M0581 <- VALUES[1, "M0581"]
  M0583 <- VALUES[1, "M0583"]
  M0783 <- VALUES[1, "M0783"]
  
  M0581_H <- VALUES[1, "M0581_H"]
  M0583_H <- VALUES[1, "M0583_H"]
  M0783_H <- VALUES[1, "M0783_H"]
  
  TEAOR25MILYEN <- VALUES[1, "TEAOR25MILYEN"]
  
  
  if(CHANGED_ALL_M003[row] %in% CHANGED_M0582_R$M003){
    
    res <- dbSendQuery(con, paste("select M0582, TO_CHAR(M0582_H, 'YYYYMMDD') M0582_H from VB.F003_M0582 where M003 = '", CHANGED_ALL_M003[row], "' and M0582_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') order by KULDES_VEGE"))
    M0582 <- fetch(res)
    dbClearResult(res)
    M0582 <- M0582[1, ]
    M0581 <- M0582$M0582
    
    if(is.na(M0581) == FALSE & nchar(M0581) == 3){
      
      M0581 <- paste("0", M0581, sep = "")
      
    }
    
    M0581_H <- M0582$M0582_H
    cat(paste("Új cég a nemzeti számla hatálykörben: ", VALUES[1, "M003"], "\n", sep = ""))
    
  }

  
  if(CHANGED_ALL_M003[row] %in% HATALYOS$M003 & !(CHANGED_ALL_M003[row] %in% CHANGED_M0582_R$M003)){
    
    M0581 <- HATALYOS[HATALYOS$M003 == CHANGED_ALL_M003[row], "M0582"]
    
    if(is.na(M0581) == FALSE & nchar(M0581) == 3){
      
      M0581 <- paste("0", M0581, sep = "")
      
    }
    
    M0581_H <- HATALYOS[HATALYOS$M003 == CHANGED_ALL_M003[row], "M0582_H"]
    cat(paste("Nemzeti számla TEÁOR kóddal és hatállyal kerül kiküldésre: ", VALUES[1, "M003"], "\n", sep = ""))
    
  }
  
  # if(CHANGED_ALL_M003[row] %in% HATALYVEGE$M003 & !(CHANGED_ALL_M003[row] %in% CHANGED_M0582_R$M003)){
  #   
  #   #& HATALYVEGE$M0582 == M0581
  #   if(length(HATALYVEGE[HATALYVEGE$M003 == CHANGED_ALL_M003[row], "M0582_HV"]) != 0){
  # 
  #     if(as.numeric(max(HATALYVEGE[HATALYVEGE$M003 == CHANGED_ALL_M003[row], "M0582_HV"])) > as.numeric(M0581_H)){
  #       
  #       M0581_H <- max(HATALYVEGE[HATALYVEGE$M003 == CHANGED_ALL_M003[row], "M0582_HV"])
  #       
  #     }
  #     
  #     
  #   }
  #   
  # }
    
  if (grepl("\"", VALUES[1, "NEV"], fixed = TRUE) || grepl("'", VALUES[1, "NEV"], fixed = TRUE)  || grepl(",", VALUES[1, "NEV"], fixed = TRUE)){
    
    NEV <- paste("\"", VALUES[1, "NEV"], "\"", sep = "")
    NEV <- paste("\"", gsub("\"", "\"\"", VALUES[1, "NEV"]), "\"", sep = "")
    
  }else{
    
    NEV <- VALUES[1, "NEV"]#6. attribútum
    
  }
  
  
  if (grepl("\"", VALUES[1, "RNEV"], fixed = TRUE) || grepl("'", VALUES[1, "RNEV"], fixed = TRUE) || grepl(",", VALUES[1, "RNEV"], fixed = TRUE)){
    
    RNEV <- paste("\"", VALUES[1, "RNEV"], "\"", sep = "")
    RNEV <- paste("\"", gsub("\"", "\"\"", VALUES[1, "RNEV"]), "\"", sep = "")
    
  }else{
    
    RNEV <- VALUES[1, "RNEV"]
    
  }
  
  
  if (grepl("\"", VALUES[1, "TELNEV_SZH"], fixed = TRUE) || grepl("'", VALUES[1, "TELNEV_SZH"], fixed = TRUE) || grepl(",", VALUES[1, "TELNEV_SZH"], fixed = TRUE)){
    # || grepl(".", VALUES[1, "TELNEV_SZH"], fixed = TRUE)
    if (grepl("\"", VALUES[1, "TELNEV_SZH"], fixed = TRUE) || grepl(",", VALUES[1, "TELNEV_SZH"], fixed = TRUE)){
      
      TELNEV_SZH <- substring(VALUES[1, "TELNEV_SZH"], 1, (nchar(VALUES[1, "TELNEV_SZH"]) - 2))
      
    }else{
      
      TELNEV_SZH <- VALUES[1, "TELNEV_SZH"]
      
    }
    
    TELNEV_SZH <- paste("\"", TELNEV_SZH, "\"", sep = "")
    cat(paste("Vesszőt tartalmazott a TELNEV_SZH: ", VALUES[1, "M003"], "\n", sep = ""))
    
  }else{
    
    TELNEV_SZH <- VALUES[1, "TELNEV_SZH"]
    
  }
  
  
  if (grepl("\"", VALUES[1, "TELNEV_LEV"], fixed = TRUE) || grepl("'", VALUES[1, "TELNEV_LEV"], fixed = TRUE)  || grepl(",", VALUES[1, "TELNEV_LEV"], fixed = TRUE)){
    
    TELNEV_LEV <- paste("\"", VALUES[1, "TELNEV_LEV"], "\"", sep = "")
    TELNEV_LEV <- paste("\"", gsub("\"", "\"\"", VALUES[1, "TELNEV_LEV"]), "\"", sep = "")
    
  }else{
    
    TELNEV_LEV <- VALUES[1, "TELNEV_LEV"]#6. attribútum
    
  }
  
  
  if (grepl("\"", VALUES[1, "UTCA_SZH"], fixed = TRUE) || grepl("'", VALUES[1, "UTCA_SZH"], fixed = TRUE) || grepl(",", VALUES[1, "UTCA_SZH"], fixed = TRUE)){
    
    UTCA_SZH <- paste("\"", VALUES[1, "UTCA_SZH"], "\"", sep = "")
    UTCA_SZH <- paste("\"", gsub("\"", "\"\"", VALUES[1, "UTCA_SZH"]), "\"", sep = "")
    
  }else{

    UTCA_SZH <- VALUES[1, "UTCA_SZH"]
    
  }
  
  
  if (grepl("\"", VALUES[1, "UTCA_LEV"], fixed = TRUE) || grepl("'", VALUES[1, "UTCA_LEV"], fixed = TRUE) || grepl(",", VALUES[1, "UTCA_LEV"], fixed = TRUE)){
    
    UTCA_LEV <- paste("\"", VALUES[1, "UTCA_LEV"], "\"", sep = "")
    UTCA_LEV <- paste("\"", gsub("\"", "\"\"", VALUES[1, "UTCA_LEV"]), "\"", sep = "")
    
  }else{
    
    UTCA_LEV <- VALUES[1, "UTCA_LEV"]
    
  }
  
  
  ALAKDAT <- VALUES[1, 29]
  if(VALUES[1, "M003"] == "15302724" || VALUES[1, "M003"] == "15736527"){
    
    ALAKDAT <- "19830101"
    cat("MNB vagy KSH alakulás dátuma 1983. január 01-re változott\n")
    
  }
  
  
  if(nrow(CHANGED_HIST_ALAKDAT) > 0){
    
    if(CHANGED_ALL_M003[row] %in% CHANGED_HIST_ALAKDAT$M003){
      
      ALAKDAT <- gsub("-", "" ,CHANGED_HIST_ALAKDAT[CHANGED_HIST_ALAKDAT$M003 == CHANGED_ALL_M003[row], "ALAKDAT_U"])
      cat(paste("Az alakulás dátuma megváltozott:", VALUES[1, "M003"], VALUES[1, 29], ALAKDAT, "\n", sep = " "))
      
    }
    
  }
  
  Q01[row, 4] <- paste("Q01", datum, "15302724", datum, "E", "KSHTORZS", paste("@KSHTORZS", KSHTORZS, sep = ""), 
                       VALUES[1, "M003"], VALUES[1, "M0491"], VALUES[1, "M0491_H"], M005_SZH, VALUES[1, "M005_SZH_H"], 
                       NEV, VALUES[1, "NEV_H"], RNEV, VALUES[1, "RNEV_H"], VALUES[1, "M054_SZH"], TELNEV_SZH, UTCA_SZH, 
                       VALUES[1, "SZEKHELY_H"], VALUES[1, "M054_LEV"], TELNEV_LEV, UTCA_LEV, 
                       VALUES[1, "LEVELEZESI_R"], VALUES[1, 18], VALUES[1, 19], PFIOK_LEV, VALUES[1, 21], 
                       VALUES[1, 22], VALUES[1, 23], M025, VALUES[1, 25],  VALUES[1, 26], ARBEV_H, 
                       M009_SZH_CDV_TOGETHER, ALAKDAT, M0781, VALUES[1, 31], M058_J, VALUES[1, 33], VALUES[1, 34], 
                       MP65_H, VALUES[1, "UELESZT"], VALUES[1, "M003_R"], VALUES[1, "DATUM"], M0581, M0581_H, CEGV, 
                       VALUES[1, "CEGV_H"], VALUES[1, "MVB39"], VALUES[1, "MVB39_H"], ORSZ, LETSZAM, ARBEV, 
                       M0783, M0783_H, M0583, M0583_H, 
                       TEAOR25MILYEN, EELERHETOSEG_10V11, EELERHETOSEG_10V11_HATALY, 
                       EELERHETOSEG_60, EELERHETOSEG_60_HATALY, sep = ",")
  
}
View(Q01)

dbDisconnect(con)
gc()

hiba <- 0
for(i in 1:nrow(Q01)){
  
  for(j in 1:ncol(Q01)){
    
    if(Q01[i, j] != CHANGED_ON_20250612[CHANGED_ON_20250612$FILENAME == Q01[i, 2] & CHANGED_ON_20250612$SORSZAM == Q01[i, 3], j]){
      
      hiba <- hiba + 1
      cat(paste(Q01[i, j], CHANGED_ON_20250612[CHANGED_ON_20250612$FILENAME == Q01[i, 2] & CHANGED_ON_20250612$SORSZAM == Q01[i, 3], j], sep = "\n"))
      cat("\n")
      cat("\n")
    }
    
  }
  #if (i == 1) break
  #if (hiba == 1) break
  
}
hiba

Q01[substr(Q01$X4, 60, 67) == '32341272', 4] # nincs benne
Q01[substr(Q01$X4, 60, 67) == '14756553', 4] # benne van
# Q01,20250604,15302724,20250604,E,KSHTORZS,@KSHTORZS0000113,14756553,113,20110101,01,20160215,\"RD West Finance Korlátolt Felelősségű Társaság \"\"felszámolás alatt\"\"\",20250529,\"RD West Finance Kft. \"\"f. a.\"\"\",20250529,1134,Budapest 13. ker.,Váci út 33.,20160215,1054,Budapest 05. ker.,Kálmán Imre u. 1.,20091105,,,,,2,20250529,00,20250101,0,20250101,24299,20090504,6492,20090504,6523,20230101,S1270,20211001,,20090505,20250530,
# 6492,20200101,0109918379,20090504,0,20160101,HU,N/A,N/A,6492,20250101,6492,20250101,0,rdwestfinance@gmail.com,20160215,14756553#cegkapu,20180614

Q01_SENT <- read.delim2(file = "Q:/mnb_ebead/elkuldott/Q015060315302724", header = FALSE)
str(Q01_SENT)
View(Q01_SENT)

for(i in 1:nrow(Q01_SENT)){
  
  print(paste(substr(Q01_SENT[i , "V1"], 60, 67), ",", sep = ""))

}

Q01_REKORD <- Q01$X4

write.table(Q01_REKORD, "Q016102315302724", sep = "\n", row.names = FALSE, col.names = FALSE, quote = FALSE)

# hiba <- 0
# for(i in 1:length(Q01_REKORD)){
#   
#   if(Q01_REKORD[i] != Q01_SENT[i, 1]){
#     
#     hiba <- hiba + 1
#     cat(paste(Q01_REKORD[i], Q01_SENT[i, 1], sep = "\n"))
#     cat("\n")
#     cat("\n")
#   }
#   
# }
# hiba

#Q0102 <- rbind(Q01, Q02)
#View(Q0102)
# Q01_RENDEZVE <- CHANGED_ON_20250423[order(CHANGED_ON_20250423$SORSZAM), ]
# View(Q01_RENDEZVE[Q01_RENDEZVE$KOD == "Q01", "REKORD"])
# write.table(Q01_RENDEZVE[Q01_RENDEZVE$KOD == "Q01", "REKORD"], "Q015042315302724", sep = "\n", row.names = FALSE, col.names = FALSE, quote = FALSE)
# write.table(CHANGED_ON_20250423[CHANGED_ON_20250423$KOD == "Q02", "REKORD"], "Q025042315302724", sep="\n", row.names = FALSE, col.names = FALSE, quote = FALSE)