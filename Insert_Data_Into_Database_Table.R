library("ROracle")

drv <- Oracle()
con <- dbConnect(drv, username = Sys.getenv("userid"), password = Sys.getenv("pwd"), dbname = "emerald.ksh.hu")

# rs <- dbSendQuery(con, "insert into VB_REP.MNB_NAPI(KOD, FILENAME, SORSZAM, REKORD) values (:1, :2, :3, :4)", data = Q0102)
# dbCommit(con)
# dbClearResult(rs)

dbDisconnect(con)