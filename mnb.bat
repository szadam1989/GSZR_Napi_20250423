# set up oracle environment
ORACLE_HOME=`grep '^oracle7:' /etc/passwd | cut -d : -f6`
. $ORACLE_HOME/.oracle.env
rm /users/VB_MEGR/megrendelesek/napi/mnb/mnbhiv.log
# EBEAD feltolto
sqlplus -s VB_MEGR/inf1AAvbMegr@emerald @/users/VB_MEGR/megrendelesek/napi/mnb/mnb_EBEAD.sql >> /users/VB_MEGR/megrendelesek/napi/mnb/mnbhiv.log
elm -s "MNB log" adam.szilagyi@ksh.hu < /users/VB_MEGR/megrendelesek/napi/mnb/mnbhiv.log
elm -s "MNB log" nora.kecskes@ksh.hu < /users/VB_MEGR/megrendelesek/napi/mnb/mnbhiv.log
elm -s "MNB log" zsuzsa.nagy@ksh.hu < /users/VB_MEGR/megrendelesek/napi/mnb/mnbhiv.log
elm -s "MNB log" krisztina.kanyo@ksh.hu < /users/VB_MEGR/megrendelesek/napi/mnb/mnbhiv.log
elm -s "MNB log" peter.molec@ksh.hu < /users/VB_MEGR/megrendelesek/napi/mnb/mnbhiv.log
elm -s "MNB log" zsolt.meszaros@ksh.hu < /users/VB_MEGR/megrendelesek/napi/mnb/mnbhiv.log

