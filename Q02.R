library("ROracle")
library("stringr")

if (nrow(CHANGED_ALL_M003_JE_JU) != 0){

  drv <- Oracle()
  con <- dbConnect(drv, username = Sys.getenv("userid"), password = Sys.getenv("pwd"), dbname = "emerald.ksh.hu")
  
  Q02 <- data.frame(matrix(NA, nrow = nrow(CHANGED_ALL_M003_JE_JU), ncol = 4))
  
  Q02[, 1] <- "Q02"
  Q02[, 2] <- paste("Q02", substr(Sys.Date(), 4, 4), substr(Sys.Date(), 6, 7), substr(Sys.Date(), 9, 10), "15302724", sep = "")
  Q02[, 3] <- c(1:nrow(Q02))
    
  View(Q02)
  
  datum <- paste(substr(Sys.Date(), 1, 4), substr(Sys.Date(), 6, 7), substr(Sys.Date(), 9, 10), sep = "")
  
  for(row in 1:nrow(CHANGED_ALL_M003_JE_JU)){
    
    KSHTORZS <- str_pad(row, width = 7, pad = "0")
    
    if(CHANGED_ALL_M003_JE_JU[row, "M003_JE"] %in% CHANGED_Q02_1$M003_JE){
      
      res <- dbSendQuery(con, paste("select M003_JE, to_char(M003_JU), MV07, MV501_JE, MV501_JU, to_char(DTOL, 'YYYYMMDD'), to_char(JEJU_R, 'YYYYMMDD'), '0' KULF_JU, null orszagkod, decode(dig ,null, '0', '1') lezarva from VB.F003_JUJE JUJE, VB.F003 FO where JUJE.M003_JE = FO.M003 and M003_JE = '", CHANGED_ALL_M003_JE_JU[row, "M003_JE"], "' and M003_JU = '", CHANGED_ALL_M003_JE_JU[row, "M003_JU"], "' and DATUM_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')"))
      VALUES <- fetch(res)
      dbClearResult(res)
      
    }else if (CHANGED_ALL_M003_JE_JU[row, "M003_JE"] %in% CHANGED_Q02_2$M003_JE){
      
      res <- dbSendQuery(con, paste("select M003_JE, to_char(M003_JU), MV07, MV501_JE, MV501_JU, to_char(DTOL, 'YYYYMMDD'), to_char(JEJU_R, 'YYYYMMDD'), '0' kulf_ju, null orszagkod, decode(dig, null, '0', '1') from VB.F003_JUJE, (select M003, param_dtol from VB_REP.VB_APP_INIT where M003 is not null) c where c.M003 = F003_JUJE.M003_JE and M003_JE = '", CHANGED_ALL_M003_JE_JU[row, "M003_JE"], "' and M003_JU = '", CHANGED_ALL_M003_JE_JU[row, "M003_JU"], "' and TO_DATE(param_dtol, 'YYYY-MM-DD hh24:MI:SS') >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')"))
      VALUES <- fetch(res)
      dbClearResult(res)
      
    }else if (CHANGED_ALL_M003_JE_JU[row, "M003_JE"] %in% CHANGED_Q02_3$M003_JE){
      
      res <- dbSendQuery(con, paste("select M003_JE, '00000001' M003_JU, null MV07,	decode(j.atalak,'A','220','B','230','E','235','K','830','L','835','O','240','S','285','V','280','990') MV501_JE, decode(j.atalak,'A','120','B','930','E','190','K','130','L','135','O','140','S','185','V','180','990') MV501_JU, to_char(g.M040K, 'YYYYMMDD') dtol, to_char(j.datum_r, 'YYYYMMDD'), kulf_ju, null orszagkod, '0' from VB_CEG.JOGUTOD j, VB.F003 g where KULF_JU = '1' and g.M003 = M003_JE and g.M040 = '9' and M003_JE = '", CHANGED_ALL_M003_JE_JU[row, "M003_JE"], "' and g.M040k_r >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH24:MI:SS')"))
      VALUES <- fetch(res)
      dbClearResult(res)
      
    }else if (CHANGED_ALL_M003_JE_JU[row, "M003_JE"] %in% CHANGED_Q02_4$M003_JE){
      
      res <- dbSendQuery(con, paste("select  distinct M003_JE, '00000001' M003_JU, null MV07, decode(j.atalak,'A','220','B','230','E','235','K','830','L','835','O','240','S','285','V','280','990') MV501_JE, decode(j.atalak,'A','120','B','930','E','190','K','130','L','135','O','140','S','185','V','180','990') MV501_JU, to_char(r.M040K, 'YYYYMMDD') dtol, to_char(j.datum_r, 'YYYYMMDD'), kulf_ju, null orszagkod, '0' from VB_CEG.JOGUTOD j, VB_REP.VB_APP_INIT g, VB.F003 r where kulf_ju = '1' and g.M003 = M003_JE and g.M003 = r.M003 and datum_r = (select max(datum_r) from VB_CEG.JOGUTOD where M003_JE = J.M003_JE) and to_date(param_dtol, 'YYYY-MM-DD HH24:MI:SS') >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH24:MI:SS')"))
      VALUES <- fetch(res)
      dbClearResult(res)
      
    }
    
    VALUES[is.na(VALUES)] <- ""
    
    if(nchar(VALUES[1, 2]) == 1){
      
      VALUES[1, 2] <- paste("0000000", VALUES[1, 2], sep = "")
      
    }
    
    Q02[row, 4] <- paste("Q02", datum, "15302724", datum, "E", "KSHJEJU", paste("@KSHJEJU", KSHTORZS, sep = ""), VALUES[1, 1], VALUES[1, 2], VALUES[1, 3], VALUES[1, 4], VALUES[1, 5], VALUES[1, 6], VALUES[1, 7], VALUES[1, 8], VALUES[1, 9], VALUES[1, 10], sep = ",")
    
  }
  View(Q02)
  
  # Q02[substr(Q02$X4, 58, 65) == "32341272", 4]
  
  M003ToFind <- paste0("'", CHANGED_ALL_M003_JE_JU$M003_JE, "'", collapse=", ")
  whereIn <- paste0("(", M003ToFind, ")")
  res <- dbSendQuery(con, paste("select M003, M040, M040K from VB.F003 where M003 in ", whereIn, sep = ""))
  CHANGED_M040K_Q02 <- fetch(res)
  dbClearResult(res)
  print(CHANGED_M040K_Q02)
  
  M003ToFind <- paste0("'", CHANGED_ALL_M003_JE_JU$M003_JU, "'", collapse=", ")
  M003ToFind <- gsub("'NA', ", "", M003ToFind)
  whereIn <- paste0("(", M003ToFind, ")")
  res <- dbSendQuery(con, paste("select M003, M040, M040K from VB.F003 where M003 in ", whereIn, sep = ""))
  CHANGED_M040K_Q02_JU <- fetch(res)
  dbClearResult(res)
  print(CHANGED_M040K_Q02_JU)
  
  dbDisconnect(con)

}


hiba <- 0
for(i in 1:nrow(Q02)){
  
  for(j in 1:ncol(Q02)){
    
    if(Q02[i, j] != CHANGED_ON_20250612[CHANGED_ON_20250612$FILENAME == Q02[i, 2] & CHANGED_ON_20250612$SORSZAM == Q02[i, 3], j]){
      
      hiba <- hiba + 1
      cat(paste(i, j, Q02[i, j], CHANGED_ON_20250612[CHANGED_ON_20250612$FILENAME == Q02[i, 2] & CHANGED_ON_20250612$SORSZAM == Q02[i, 3], j], sep = "\n"))
      
    }
    
  }
  
}
hiba

Q02_SENT <- read.delim(file = "Q:/mnb_ebead/elkuldott/Q026102315302724", header = FALSE)
str(Q02_SENT)
View(Q02_SENT)

# hiba <- 0
# for(i in 1:nrow(Q02)){
#   
#   if(Q02[i, 4] != Q02_SENT[i, 1]){
#     
#     hiba <- hiba + 1
#     cat(paste(Q02[i, 4], Q02_SENT[i, 1], sep = "\n"))
#     cat("\n")
#     cat("\n")
#   }
#   
# }
# hiba

Q02_REKORD <- Q02$X4
write.table(Q02_REKORD, "Q025042315302724", sep="\n", row.names = FALSE, col.names = FALSE, quote = FALSE)