library("ROracle")
drv <- Oracle()
con <- dbConnect(drv, username = Sys.getenv("userid"), password = Sys.getenv("pwd"), dbname = "emerald.ksh.hu")
res <- dbSendQuery(con, "select * from VB_REP.VB_APP_INIT where ALKALMAZAS like 'MNB napi változáslista küldése' and PROGRAM = 'mnb_EBEAD.sql' and PARAM_NEV = 'utolso_futas'")
LAST_RUNNING <- fetch(res)
(LAST_RUNNING$PARAM_ERTEK) # 2025-06-11 10:30:01
dbClearResult(res)
#LAST_RUNNING$PARAM_ERTEK <- "2025-04-22 10:30:00"

res <- dbSendQuery(con, "select * from VB_REP.MNB_NAPI") # _DEBUG
CHANGED_ON_20250612 <- fetch(res)
dbClearResult(res)
View(CHANGED_ON_20250612[CHANGED_ON_20250612$KOD == "Q01",])
View(CHANGED_ON_20250612[CHANGED_ON_20250612$KOD == "Q02",])

res <- dbSendQuery(con, "select M003, datum from VB.F003_HIST3PR where alakdat != alakdat_u order by DATUM desc")
HIST_ALAKDAT <- fetch(res)
dbClearResult(res)
View(HIST_ALAKDAT)
res <- dbSendQuery(con, "select M003, datum from VB.F003_HIST3PR where alakdat != alakdat_u and M003 in (select M003 from VB.F003 where substr(M0491, 1, 2) != '23' and M0491 != '961' ) order by DATUM desc")
HIST_ALAKDAT_SZUKEBB <- fetch(res)
dbClearResult(res)
View(HIST_ALAKDAT_SZUKEBB)
res <- dbSendQuery(con, "select M003, UELESZT, UELESZT_R from VB.F003 where UELESZT is not null and substr(M0491, 1, 2) != '23' and M0491 != '961'  order by UELESZT_R desc")
HIST_UELESZT <- fetch(res)
dbClearResult(res)
View(HIST_UELESZT)


#Q01
res <- dbSendQuery(con, paste("select M003, M003_R from VB.F003 where M003_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
NEW_M003 <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, M0491_R from VB.F003 where M0491_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and M0491_F != '06' and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_M0491_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, M005_SZH_R from VB.F003 where M005_SZH_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_M005_SZH_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, NEV_R from VB.F003 where NEV_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_NEV_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, RNEV_R from VB.F003 where RNEV_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_RNEV_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, SZEKHELY_R from VB.F003 where SZEKHELY_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_SZEKHELY_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, LEVELEZESI_R from VB.F003 where LEVELEZESI_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_LEVELEZESI_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, LEV_PF_R from VB.F003 where LEV_PF_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_LEV_PF_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, M040K_R from VB.F003 where M040K_R > TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_M040K_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, M040V_R from VB.F003 where M040V_R > TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_M040V_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, LETSZAM_R from VB.F003 where LETSZAM_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_LETSZAM_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, ARBEV_R from VB.F003 where ARBEV_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_ARBEV_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, M0783_R from VB.F003 where M0783_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_M0783_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, M0583_R from VB.F003 where M0583_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_M0583_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, MP65_R from VB.F003 where MP65_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_MP65_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, UELESZT_R from VB.F003 where UELESZT_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_UELESZT_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, CEGV_R from VB.F003 where CEGV_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_CEGV_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, MVB39_R from VB.F003 where MVB39_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_MVB39_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select * from VB.F003_M0582 where M0582_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')"))
CHANGED_M0582_R <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, M0582, TO_CHAR(M0582_HV, 'YYYYMMDD') M0582_HV from VB.F003_M0582 where M003 not in (select M003 from VB.F003_M0582 where M0582_HV is null) order by M003"))
HATALYVEGE<- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, M0582, TO_CHAR(M0582_H, 'YYYYMMDD') M0582_H from VB.F003_M0582 where M0582_HV is null order by M003"))
HATALYOS <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, datum, alakdat_u from VB.F003_HIST3PR where datum >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and alakdat != alakdat_u and M003 in (select M003 from VB.F003 where substr(M0491, 1, 2) != '23' and M0491 != '961')"))
CHANGED_HIST_ALAKDAT <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, param_dtol from VB_REP.VB_APP_INIT where program = 'mnb_EBEAD.sql' and TO_DATE(param_dtol, 'YYYY-MM-DD HH24:MI:SS') >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and param_nev = 'm003'"))
PLUS_M003 <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003, param_dtol from VB_REP.VB_APP_INIT where program = 'mnb_EBEAD.sql' and TO_DATE(param_dtol, 'YYYY-MM-DD HH24:MI:SS') >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and param_nev = '-m003'"))
MINUS_M003 <- fetch(res)
dbClearResult(res)


#Q02
res <- dbSendQuery(con, paste("select M003_JE, M003_JU from VB.F003_JUJE JUJE, VB.F003 FO where JUJE.M003_JE = FO.M003 and (JUJE.JEJU_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') or JUJE.DATUM_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')) and substr(M0491, 1, 2) != '23' and M0491 != '961'"))
CHANGED_Q02_1 <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003_JE, M003_JU from VB.F003_JUJE JUJE, (select M003, PARAM_DTOL from VB_REP.VB_APP_INIT where M003 is not null) c where JUJE.M003_JE = c.M003 and TO_DATE(PARAM_DTOL, 'YYYY-MM-DD HH24:MI:SS') >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')"))
CHANGED_Q02_2 <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select M003_JE, M003_JU from VB_CEG.JOGUTOD j, VB.F003 g where kulf_ju = '1' and g.M003 = M003_JE and g.M040K_R >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS') and g.M040 = '9'"))
CHANGED_Q02_3 <- fetch(res)
dbClearResult(res)

res <- dbSendQuery(con, paste("select distinct M003_JE, M003_JU from VB_CEG.JOGUTOD j, VB_REP.VB_APP_INIT g, vb.f003 r where KULF_JU = '1' and g.M003 = M003_JE and g.M003 = r.M003 and datum_r = (select max(datum_r) from VB_CEG.JOGUTOD where M003_JE = j.M003_JE) and TO_DATE(param_dtol, 'YYYY-MM-DD HH24:MI:SS') >= TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')"))
CHANGED_Q02_4 <- fetch(res)
dbClearResult(res)

#Q01
CHANGED_ALL_M003 <- c(NEW_M003$M003, CHANGED_M005_SZH_R$M003, CHANGED_NEV_R$M003, CHANGED_RNEV_R$M003, CHANGED_SZEKHELY_R$M003, CHANGED_LEVELEZESI_R$M003, CHANGED_LEV_PF_R$M003, CHANGED_M040K_R$M003, CHANGED_M040V_R$M003, CHANGED_LETSZAM_R$M003, CHANGED_ARBEV_R$M003, CHANGED_M0783_R$M003, CHANGED_M0583_R$M003, CHANGED_MP65_R$M003, CHANGED_UELESZT_R$M003, CHANGED_CEGV_R$M003, CHANGED_MVB39_R$M003, CHANGED_M0491_R$M003, CHANGED_HIST_ALAKDAT$M003) #CHANGED_M063_R$M003 #CHANGED_M0582_R$M003,, PLUS_M003$M003
CHANGED_ALL_M003 <- unique(CHANGED_ALL_M003)

MennyiEzer <- as.integer(length(CHANGED_ALL_M003) / 1000)
tartomanyKezdet <- 1
if(MennyiEzer == 0){
  
  tartomanyVeg <- length(CHANGED_ALL_M003)
  
}else{
  
  tartomanyVeg <- 1000
  
}

#MennyiEzer + 1
for(tartomany in 0:MennyiEzer+1){
  print(tartomany)
  M003ToFind <- paste0("'", CHANGED_ALL_M003[c(tartomanyKezdet:tartomanyVeg)], "'", collapse=", ")
  whereIn <- paste0("(", M003ToFind, ")")
  
  res <- dbSendQuery(con, paste("select M003, M040K from VB.F003 where M040 in ('0','9') and TO_CHAR(M040K, 'YYYY') <> '", substr(Sys.Date(), 1, 4), "' and M003 in ", whereIn, " and M040K_R < TO_DATE('", LAST_RUNNING$PARAM_ERTEK, "', 'YYYY/MM/DD HH:MI:SS')" , sep = ""))
  CHANGED_M040K <- fetch(res)
  dbClearResult(res)
  
  CHANGED_ALL_M003 <- CHANGED_ALL_M003[!(CHANGED_ALL_M003 %in% CHANGED_M040K$M003)]
  print(CHANGED_M040K)
  
  tartomanyKezdet <- tartomanyKezdet + 1000
  
  if(tartomany < MennyiEzer){
    
    tartomanyVeg <- tartomanyVeg + 1000
    
  }else{
    
    tartomanyVeg <- length(CHANGED_ALL_M003)
    
  }
  
}

dbDisconnect(con)


str(CHANGED_ALL_M003)
CHANGED_ALL_M003 <- CHANGED_ALL_M003[order(CHANGED_ALL_M003)]
View(CHANGED_ALL_M003)
length(CHANGED_ALL_M003)
#CHANGED_ALL_M003 <- PLUS_M003$M003
# CHANGED_ALL_M003 <- c(CHANGED_ALL_M003, PLUS_M003$M003, CHANGED_M0582_R$M003)
# length(CHANGED_ALL_M003)
# CHANGED_ALL_M003 <- unique(CHANGED_ALL_M003)
# length(CHANGED_ALL_M003)
# CHANGED_ALL_M003 <- CHANGED_ALL_M003[order(CHANGED_ALL_M003)]


#Q02
CHANGED_ALL_M003_JE_JU <- rbind(CHANGED_Q02_1, CHANGED_Q02_2, CHANGED_Q02_3, CHANGED_Q02_4)
CHANGED_ALL_M003_JE_JU <- unique(CHANGED_ALL_M003_JE_JU)
CHANGED_ALL_M003_JE_JU <- CHANGED_ALL_M003_JE_JU[order(CHANGED_ALL_M003_JE_JU$M003_JE, CHANGED_ALL_M003_JE_JU$M003_JU), ]
dim(CHANGED_ALL_M003_JE_JU)
str(CHANGED_ALL_M003_JE_JU)
View(CHANGED_ALL_M003_JE_JU)

for(i in 1:nrow(CHANGED_ALL_M003_JE_JU)){
  
  if(CHANGED_ALL_M003_JE_JU[i, "M003_JE"] == CHANGED_ALL_M003_JE_JU[i, "M003_JU"]){
    
    print("A jogelőd és a jogutód megegyezik.")
    
  }
  
}