set termout on
set linesize 1000
set trimspool on
set heading off
set echo off
set serveroutput on size 1000000

alter session set plsql_ccflags='debug_on:false';--true

DECLARE
    verzio varchar2(25):=' 70. verzió';
    PLUSZ_Q01        number:=0; 
    MINUSZ_Q01       number := 0;
    ALAKDAT_MODSZAM  number:=0; 
    M003_ALAKDAT_CHANGED varchar2(8 char);
    sorok            number:=0;   
    sorok1           number:=0;
	puffersorok      number:=0;
    datum_kar            varchar2(21);
    datum            date;         
    utolso_futas     date;    
	utolso_kuldes    date;
    w_cdv            varchar2(1);
    w_m025           varchar2(2):='  ';
    w_m026           varchar2(1):=' ';
    wm0581 varchar2(4):=null;
    sor              varchar2(1000);
    q01db            number;
    q02db            number;
    q01mellekletnev  varchar2(30);
    q02mellekletnev  varchar2(30);
    napok_szama_visszamenoleg number:=1;  -- alapértelmezett a tegnapi nap óta
    serr             number:=0;               --hibakód
    errmsg  varchar2(100);                 -- hibaüzenet
    alsohatar  date :=to_date('19000101','YYYYMMDD');
    felsohatar date :=to_date('24991231','YYYYMMDD');
    kihagyott  number:=0;
    ures_kuldes number:=0;
    mehet boolean:=false;
    megszunt boolean:=false;
    levalogatva_1 number:=0;
	levalogatva_2 number:=0;
    nem_valtozott number:=0;
	osszesq01 number:=0;
    kulondb number:=0;      --a külön listán kért törzsszámok száma
    programneve varchar(40);
    tablaneve varchar2(40):=$if $$debug_on $then 'vb_rep.mnb_napi_debug'; $else 'vb_rep.mnb_napi'; $end
    w_statteaor varchar2(4);
    w_stathataly date;
	w_hatalyvege date;
	w_stat_r date;
	w_kuldes_vege date;
	w_m003 number;
	w_alakdat date:=null;
    uzenet varchar2(60):='';
    lezarasdb number:=0;
    rownumber number;
    maxm0581r date;
    rekordsorszam number:=0;
--konstansok
    nev_hossz number:=250;  --2016.05.23.-tól, már az 50-es verzióban is.
	rnev_hossz number:=250;  --az okozza az eltérést, hogy a rövid nevet eddig nem vágtam le!
	utca_hossz number:=80;
	telnev_hossz number:=20;
	pfiok_lev_hossz number:=20;
	regi_datum date:=to_date('19000101','YYYYMMDD'); 
	
	m003_r_db       number:=0;
	m005_szh_db     number:=0;
	nev_r_db        number:=0;
	rnev_r_db       number:=0;
	szekhely_r_db   number:=0;
	levelezesi_r_db number:=0;
	LEV_PF_R_db     number:=0;
	m040k_r_db	    number:=0;
	m040v_r_db      number:=0;
	letszam_h_db    number:=0;
	arbev_r_db      number:=0;
	m0783_r_db      number:=0;
	m0583_r_db      number:=0;
	MP65_r_db       number:=0; --m063_r_db
	ueleszt_db      number:=0;
	m0491_r_db      number:=0;
	cegv_db         number:=0;
	ifrs_db         number:=0;
    m003_r_db_for_rnev       number:=0;
    m003_r_db_for_levelezesi       number:=0;
    m003_r_db_for_lev_pf       number:=0;
    m003_r_db_for_MP65       number:=0; --m003_r_db_for_m063
    m003_r_db_for_cegv       number:=0;
	cursor gszr_cur (utso_futas date, programneve_1 varchar) is  select rownum rn, m003 from
	                (select m003 from vb.f003 where 
	                     (greatest(nvl(m003_r, regi_datum),
						          nvl(m005_szh_r, regi_datum),
								  nvl(nev_r, regi_datum),
								  nvl(rnev_r, regi_datum),
								  nvl(szekhely_r, regi_datum),
								  nvl(levelezesi_r, regi_datum),
						          nvl(LEV_PF_R, regi_datum),      
						          nvl(m040k_r, regi_datum),
								  nvl(m040v_r, regi_datum),
								  nvl(letszam_R, regi_datum),--Szilágyi Ádám 2022.10.19. _R _h helyett
								  nvl(arbev_r, regi_datum),
								  nvl(m0783_r, regi_datum),
								  nvl(m0583_r, regi_datum),
								  nvl(MP65_r, regi_datum), --m063_r --Szilágyi Ádám 2023.09.27.
								  nvl(ueleszt_R, regi_datum),--Szilágyi Ádám 2022.10.18. _R
								  nvl(cegv_r, regi_datum),
								  nvl(mvb39_r, regi_datum)
								  )>= utso_futas
								  or (m0491_r>= utso_futas and m0491_f!='06')
                         )						
                     and
                        substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811' 
                    --    and M0491 = '113' and M040 not in ('0', '9') --csak teszteléshez
                    union
                        select m003 from 
						vb.f003_m0582
						where (nvl(m0582_r,regi_datum)>=utso_futas)
					union
                        select m003 from vb.f003_hist3pr where datum>utso_futas and alakdat!=alakdat_u and M003 in (select M003 from VB.F003 where substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811')			
	                union				   
                        select m003 from vb_rep.vb_app_init where 
							   program='mnb_EBEAD.sql'--programneve_1
							   and to_date(param_dtol,'YYYY-MM-DD HH24:MI:SS')>utso_futas
							       and param_nev='m003'
					minus
                        select m003 from vb_rep.vb_app_init where	--ezeket a törzsszámokat kihagyja az adatküldésbõl (egyszer)				
							   program = 'mnb_EBEAD.sql'--programneve_1
							   and to_date(param_dtol,'YYYY-MM-DD HH24:MI:SS') > utso_futas
							       and param_nev = '-m003' 
							   );

						
	TYPE gszr_table IS TABLE OF gszr_cur %ROWTYPE
    INDEX BY PLS_INTEGER;
	gszr_rec gszr_cur%rowtype;				   
    Type q01_rec_type is record (Q01 varchar2(3 char),                        -- adatgyûjtés kódja
	                             datum1 varchar2(8 char),                    -- vonatkozási idõ 8 hosszon
                                 KSH_torzsszam varchar2(8 char),             -- KSH törzsszáma
                                 kitoltes_datum varchar2(8 char),            -- kitöltés dátuma 8 hosszon
                                 kshtorzs varchar2(20 char),                 -- 1 karakter fixen: E, táblakód: @KSHTORZS
                                 rekordsorszam varchar2(7 char),             -- sorszám 7 karakteren elõnullázva  a kurzor rekordszámából levonva az eddig átlépett rekordok számát
                                 torzsszam varchar2(8 char),                 -- törzsszám
                                 gfo varchar2(3 char),                       -- gazd.forma
								 gfo_hataly varchar2(8 char),                -- gfo hatály dátuma
								 megyekod varchar2(2 char),                  --székhely megyekódja, 
                                 megyekod_hataly varchar2(8 char),           --a megyekód hatály dátuma
                                 nev   varchar2(250 char),                   --név
                                 nev_h varchar2(8 char),                     --név hatálya
                                 rnev  varchar2(250 char),                    --rövid név
                                 RNEV_H varchar2(8 char),                    --rövid név hatálya
                                 M054_SZH varchar2(4 char),                  --székhely irányító szám
                                 telnev_szh varchar2(30 char),--20 volt Szilágyi Ádám
                                 utca_szh varchar2(100 char),--80 volt Szilágyi Ádám
                                 SZEKHELY_H varchar2(8 char),                --székhely cím hatálya            
                                 M054_LEV  varchar2(4 char),                 --levelezési cím irányító száma
                                 telnev_lev   varchar2(30 char),--20 volt Szilágyi Ádám
                                 utca_lev varchar2(80 char),                             
                                 LEVELEZESI_R varchar2(8 char),              --levelezési cím hatálya (rendszerbe kerülése)
                                 m054_pf_lev varchar2(4 char),               -- postafiókos levelezési cím irányító száma 
                                 telnev_pf_lev varchar2(30 char),  --20 volt Szilágyi Ádám          -- postafiókos levelezési cím település neve
                                 pfiok_lev varchar2(10 char),                 -- postafiók
                                 leV_PF_R varchar2(8 char),                  -- pf. cím hatálya (rendszerbe kerülése)
                                 M040 varchar2(1 char),                      -- mûködés állapotkódja 
								 m040k varchar2(8 char),                     -- állapotkód hatálya
                                 mukodv varchar2(8 char),                    -- mûködés vége
                                 m025 varchar2(2 char),                      -- létszám kategória
                                 letszam_h varchar2(8 char),                 --létszám kategória besorolás dátuma
                                 m026 varchar2(1 char),                      --árbevétel kategória
                                 arbev_h varchar2(8 char),                   --árbevétel kategória hatálya
                                 m009_szh varchar2(5 char),                  --székhely település kód+cdv 5 jegyen
                                 alakdat varchar2(8 char),                    --alakulás dátuma
                                 m0781 varchar2(4 char),                     --admin szakág 2008
                                 m0781_h varchar2(8 char),                   --2008-as besorolási kód hatálya
                                 m058_j varchar2(4 char),                    --janus TEÁOR
                                 m0581_j_h varchar2(8 char),                 --janus TEÁOR hatálya (azonos a stat TEÁOR hatályával)
                                 MP65 varchar2(5 char),   --m063 varchar2(2) --ESA szektorkód
                                 MP65_H varchar2(8 char),       --m063_h     --ESA szektorkód hatálya
                                 ueleszt   varchar2(8 char),                 --újraélesztés hatálya
                                 m003_r varchar2(8 char),                    --rendszerbe kerülés dátuma
                                 datum2 varchar2(8 char),                     --napi leválogatás dátuma
                                 statteaor varchar2(4 char),                 --statisztikai TEÁOR
                                 stathataly varchar2(8 char),                --statisztikai TEÁOR hatálya
								 cegjegyz   varchar2(20 char),                --cégjegyzék szám
								 cegjegyz_h  varchar2(8 char),               --cégjegyzékszám hatálya
								 mvb39       varchar2(1 char),               --IFRS nyilatkozat
								 mvb39_h     varchar2(8 char),                --IFRS nyilatkozat hatálya
                                 ORSZ varchar2(2 char),                         --Országkód
                                 LETSZAM varchar2(6 char),                      --Létszám
                                 ARBEV varchar2(8 char),                        --Árbevétel
                                 M0783 varchar2(4 char),                     --Adminisztratív TEÁOR 2025
                                 M0783_H varchar2(8 char),                   --2025-ös besorolási kód hatálya
                                 M0583 varchar2(4 char),                     --Statisztikai TEÁOR 2025
                                 M0583_H varchar2(8 char),                   --2025-ös besorolási kód hatálya
                                 stat_szakag_típus varchar2(2 char),        --0 = Statisztikai TEÁOR, 1 = nemzeti számlás TEÁOR
                                 EMAIL varchar2(90 char),                     --E-mail cím
                                 EMAIL_H varchar2(8 char),                   --E-mail cím hatály kezdete
                                 Cegkapu varchar2(90 char),                     --Cégkapu
                                 Cegkapu_H varchar2(8 char),                   --Cégkapu hatály kezdete
                                --rendszerbe kerülések a feldolgozáshoz
                                 m003_r_date date,
								 m0491_r  date,
								 m005_szh_r date,
								 nev_r date,
								 rnev_r date,
								 szekhely_r date,
								 m040k_r date,
								 m040v_r date,
								 m0783_r date,
								 m0583_r date,
								 MP65_r date, --m063_r
								 arbev_r date,
								 levelezesi_r_date date,
								 lev_pf_r_date date,
                                 letszam_h_date date,
								 cegjegyz_r date,
								 mvb39_r date,
                                 UELESZT_R date--Szilágyi Ádám 2022.10.19.
	                            );
    mnb_rec q01_rec_type;
	nsz_db number;                                                      -- hány nemzeti számlás teáor-rekordja van a szervezetnek?
	sqlmsg varchar2(50);
	eddig number:=0;
	kulf_db number:=0; --határon átnyúló megszûnések határon túli jogutódjainak darabszáma

BEGIN
	programneve:= $if $$debug_on $then 'mnb_EBEAD_debug.sql' $else 'mnb_EBEAD.sql' $end;
	$if $$debug_on $then dbms_output.put_line('DEBUG mód bekapcsolva'); $end
	
    select to_char(sysdate,'YYYY-MM-DD HH24:MI:SS'),sysdate into datum_kar,datum from dual;
 
    eddig:=1;      
	begin
	  $if $$debug_on $then
	    vb.mod_szam_tolt('K865', 'vb_rep.mnb_napi', sorok, 'MNB napi változáslista küldése,debug mód ', programneve || verzio,datum, 'K');
	  $else
	    vb.mod_szam_tolt('K865', 'vb_rep.mnb_napi', sorok, 'MNB napi változáslista küldése', programneve || verzio,datum, 'K');
	  $end
        commit;
    exception when others then
        serr:=sqlcode;
        insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ: '||datum_kar,'mod_szam_tolt a hiba');
        commit; 
    end;
    
	eddig:=2;
    begin        
      --  $if $$debug_on $then
       --     select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_kuldes from vb_rep.vb_app_init
        --            where alkalmazas='MNB napi változáslista küldése' 
         --            and   program='mnb_kuld.sql'
          --           and param_nev='utolso_futas - debug';			
       -- $else
            select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_kuldes from vb_rep.vb_app_init
                    where alkalmazas='MNB napi változáslista küldése' 
                     and   program='mnb_kuld.sql'
                     and param_nev='utolso_futas';
       -- $end                                            
    exception when others then 
	    dbms_output.put_line(sqlerrm);
		dbms_output.put_line('A vb_app_init táblából az utolsó küldés dátumát nem lehetett olvasni');
        utolso_kuldes:=sysdate-1;
    end; 
    
	eddig:=3;
 --  Mikor futott utoljára a lekérdezés?
    begin        
        $if  $$debug_on $then
            --dbms_output.put_line('utolso_futasba-debug');
            select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_futas from vb_rep.vb_app_init
                    where alkalmazas='MNB napi változáslista küldése' 
                     and   program='mnb_EBEAD.sql'
                     and param_nev='utolso_futas - debug';
        $else
            --dbms_output.put_line('utolso_futasba-éles');
            select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_futas from vb_rep.vb_app_init
                    where alkalmazas='MNB napi változáslista küldése'
                     and   program='mnb_EBEAD.sql'
                     and param_nev='utolso_futas';
        $end                                            
    exception when others then 
        utolso_futas:=sysdate-1;
    end;    

    dbms_output.put_line('Utolsó leválogatás: ' || to_char(utolso_futas, 'YYYY-MM-DD HH24:MI:SS') || '.');	
    dbms_output.put_line('Utolsó feltöltés: ' || to_char(utolso_kuldes, 'YYYY-MM-DD HH24:MI:SS') || '.');	
	eddig:=4;
      
--törölni az utolsó sikeres feltöltésnél régebbi rekordokat
     begin
            delete from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where 
			substr(rekord,5,8)<=to_char($if $$debug_on $then utolso_futas $else utolso_kuldes $end,'YYYYMMDD');
            commit;
     exception when others then  
            serr:=sqlcode;           
            insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ '||programneve||datum_kar, 'régi elküldött rekord törlés hiba'||to_char(utolso_kuldes,'YYYYMMDD'));
            commit; 
     end; 
     eddig:=5;
--   rekordtípus nevének összerakása(a mellékletnevek összerakása )      
    q01mellekletnev:='Q01'||substr(to_char(datum,'YYYYMMDD'),4,5)||'15302724';
    q02mellekletnev:='Q02'||substr(to_char(datum,'YYYYMMDD'),4,5)||'15302724';
       
--törölni az esetleges üres küldendõket, hogy ne legyen 2 elsõ sor!
    begin
        delete from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where q01mellekletnev=filename and substr(rekord,32,1)='N';
        delete from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where q02mellekletnev=filename and substr(rekord,32,1)='N';
        commit;
    exception when others then    
        serr:=sqlcode;
        insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ '||programneve||datum_kar,'puffersor törlés hiba');
        commit; 
    end; 
    commit;
    eddig:=6;
    
    begin
                    select count(*) into PLUSZ_Q01
                    from vb_rep.vb_app_init where 
							   program = 'mnb_EBEAD.sql'--programneve_1
							   and to_date(param_dtol, 'YYYY-MM-DD HH24:MI:SS') > utolso_futas
							       and param_nev = 'm003';
                     if (PLUSZ_Q01 > 0) then
                        dbms_output.put_line('A Q01-es adatátadással kiküldendõ plusz cégek száma: ' || PLUSZ_Q01 || '.');
                     end if;
                     
                exception
                    when no_data_found then
                        dbms_output.put_line('Kivétel történt a plusz cégek számának meghatározása során.');
                end;
                eddig:=120;


   begin    
                select count(*) into MINUSZ_Q01
                    from vb_rep.vb_app_init where 
							   program = 'mnb_EBEAD.sql'--programneve_1
							   and to_date(param_dtol, 'YYYY-MM-DD HH24:MI:SS') > utolso_futas
							       and param_nev = '-m003';
                     if (MINUSZ_Q01 > 0) then
                        dbms_output.put_line('A Q01-es adatátadásból kivonandó cégek száma: ' || MINUSZ_Q01 || '.');
                     end if;
                     
                exception
                    when no_data_found then
                        dbms_output.put_line('Kivétel történt a kivonandó cégek számának meghatározása során.');
                end;
                eddig := 121;
                
                
    begin
                    select count(*) into ALAKDAT_MODSZAM
                    from vb.f003_hist3pr where datum > utolso_futas and alakdat != alakdat_u and M003 in (select M003 from VB.F003 where substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811');
                     
                exception
                    when no_data_found then
                        dbms_output.put_line('Kivétel történt az alakulás dátumának változásának számának meghatározása során.');
                end;
                eddig:=130;
 

 --küldés vége azokra a lezárt küldendõ m0582-kre, amelyekre az elõzõ futás óta megváltozott az m0581, és az nem egyenlõ az m0582-vel
    for i in (--select t.rowid rn,t.m003 from vb.f003 g, $if $$debug_on $then vb.f003_m0582_debug $else vb.f003_m0582 $end t where 
	           select t.rowid rn,t.m003 from vb.f003 g,  vb.f003_m0582 t where
                  t.m003=g.m003 and m0582_hv is not null and t.kuldes_vege is null
                  and g.m0583_r>=utolso_futas --stat TEÁOR módosult
                  union
                  --select t.rowid rn,t.m003 from vb.f003 g,$if $$debug_on $then vb.f003_m0582_debug $else vb.f003_m0582 $end t where 
				  select t.rowid rn,t.m003 from vb.f003 g, vb.f003_m0582 t where 
                  t.m003=g.m003 and t.m0582_r>=utolso_futas and t.kuldes_vege is null)   --NSZ TEÁOR (vagy hatálya) módosult
    loop
        begin   
				$if $$debug_on $then 
				    null;
				$else	
				update  vb.f003_m0582 
					set kuldes_vege=datum,
						   kuldendo='0'
					where i.rn=rowid;
				$end	
        exception
            when others then
                serr:=sqlcode;
                INSERT INTO VB.VB_UZENET VALUES(serr,programneve||datum_kar,'Valami gáz van a lezárással:'||TO_CHAR(I.M003));
                commit;
                rollback;               
        end; 
		eddig:=161;
        sorok:=sorok+1;               
        if mod(sorok,100)=0 then
            begin
			$if $$debug_on $then
                vb.mod_szam_tolt('K865', 'vb_rep.mnb_napi', sorok, 'MNB napi változáslista küldése,debug mód ', programneve || verzio, datum, 'M');
			$else
			    vb.mod_szam_tolt('K865', 'vb_rep.mnb_napi', sorok, 'MNB napi változáslista küldése', programneve || verzio, datum, 'M');
			$end
            exception when others then
                insert into vb.vb_uzenet values (serr,programneve||datum_kar,'mod_szam_tolt a hiba');
            end;
            commit;
        end if;                 
    end loop;
    if(sorok > 0) then
        dbms_output.put_line('Nemzeti számla TEÁOR kód változások száma: ' || to_char(sorok) || '.');
    end if;
	eddig:=7;
    sorok:=0;
    nsz_db:=0;
    begin
        select nvl(max(sorszam),0) into sorok from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where q01mellekletnev=filename and substr(rekord,32,1)='E';
	exception
        when no_data_found then 
            sorok:=0;
	end; 
    
	for i in (select filename, count(*)  db from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end group by filename order by filename)
	loop
	    dbms_output.put_line(i.filename||':'||to_char(i.db));
	end loop;
	eddig:=8;
    begin
        select nvl(max(sorszam),0) into sorok1 from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where q02mellekletnev=filename;
    exception
        when no_data_found then 
            sorok1:=0;
    end;
	eddig:=9;
    if (sorok > 0 or sorok1 > 0) then
        dbms_output.put_line('A táblában található - már leválogatott - Q01-es és Q02-es sorok száma:');
        dbms_output.put_line('Már táblában van korábbról Q01-es: ' || to_char(sorok));
        dbms_output.put_line('Már táblában van korábbról Q02-es: ' || to_char(sorok1));
    end if;
    
	open gszr_cur(utolso_futas,programneve);
	--dbms_output.put_line('kurzor nyitva');
	fetch gszr_cur into gszr_rec;
	--dbms_output.put_line('fetch kész');
    if gszr_cur%notfound then
        null;
    else 
	--ha szerverhiba miatt nem futott a program, a visszamenõ napokra üres üzeneteket kell képezni
	
	if sysdate-utolso_futas >2 then
	    dbms_output.put_line('Az utolsó leválogatás óta eltelt napok száma: ' || to_char(sysdate-utolso_futas) || '.');
		for nap in 1..floor(sysdate-utolso_futas)
		loop
		    puffersorok:=puffersorok+1;
		end loop;
		commit;
	end if;
	--Innen pedig jön az aktuális változások leválogatása
        --levalogatva:=sorok;
        rekordsorszam:=1;
		--dbms_output.put_line(to_char(rekordsorszam));
		eddig:=10;
        loop
            exit when gszr_cur%notfound;
			w_m003:=gszr_rec.m003;
            mnb_rec.Q01:='Q01';                                      -- adatgyûjtés kódja
            mnb_rec.datum1:=to_char(datum,'YYYYMMDD');               -- vonatkozási idõ 8 hosszon
            mnb_rec.KSH_torzsszam:='15302724';                       -- KSH törzsszáma
            mnb_rec.kitoltes_datum:=to_char(datum,'YYYYMMDD');       -- kitöltés dátuma 8 hosszon
            mnb_rec.kshtorzs:='E,KSHTORZS,@KSHTORZS';                -- 1 karakter fixen: E, táblakód: @KSHTORZS
            mnb_rec.rekordsorszam:=substr(to_char(rekordsorszam,'0999999'),2,7);             -- sorszám 7 karakteren elõnullázva  a kurzor rtekordszámából levonva az eddig átlépett rekordok számát
 		   
			w_statteaor:=null;
			w_stathataly:=null;
			w_hatalyvege:=null;
			w_stat_r:=null;
			w_kuldes_vege:=null;
			--dbms_output.put_line(to_char(gszr_rec.m003));
                begin
                    select 
					    to_char(gszr_rec.m003),         -- törzsszám, 
                        m0491,
                        to_char(m0491_h, 'YYYYMMDD'),    --GFO hatály
                        m005_szh,                       --székhely megyekódja, 
                        to_char(m005_szh_h, 'YYYYMMDD'), --a megyekód hatály dátuma
                        case when instr(nev, '''')>0 or instr(nev, '"')>0 or instr(nev, ',')>0 then
                          '"'||replace(substr(nev, 1, nev_hossz - (REGEXP_COUNT(nev, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(nev, 1, nev_hossz)
		                end,                           -- név
   					    to_char(nev_h, 'YYYYMMDD'),     --név hatálya
                        case when instr(rnev, '''')>0 or instr(rnev,'"')>0 or instr(rnev,',')>0 then
                          '"'||replace(substr(rnev, 1, rnev_hossz - (REGEXP_COUNT(rnev, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(rnev, 1, rnev_hossz)
		                end,                         --rövid név
                        to_char(rnev_h, 'YYYYMMDD'),  --rövid név hatálya
                        rtrim(to_char(m054_szh)),    --székhely irányító szám
                        case when instr(telnev_szh, '''')>0 or instr(telnev_szh,'"')>0 or instr(telnev_szh,',')>0 then
                          '"'||replace(substr(telnev_szh, 1, telnev_hossz - (REGEXP_COUNT(telnev_szh, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(telnev_szh,1,telnev_hossz)
		                end,                                   --székhely
                        case when instr(utca_szh,'''') > 0 or instr(utca_szh,'"')>0 or instr(utca_szh,',')>0 then
                          '"'||replace(substr(utca_szh, 1, utca_hossz - (REGEXP_COUNT(utca_szh, '"') + 2)),'"', '""')||'"'
		                else
		                   substr(utca_szh,1,utca_hossz)
		                end,                                    --székhely utca, házszám
                        to_char(szekhely_h,'YYYYMMDD'),         --székhely cím hatálya   		
                        rtrim(to_char(M054_LEV)),               --levelezési cím irányító száma
                        case when instr(telnev_lev, '''') > 0 or instr(telnev_lev, '"') > 0 or instr(telnev_lev, ',') > 0 then
                          '"'||replace(substr(telnev_lev, 1, telnev_hossz - (REGEXP_COUNT(telnev_lev, '"') * 2 + 2)),'"','""')||'"'
		                else
		                   substr(telnev_lev, 1, telnev_hossz)
		                end,                                     --levelezési cím település név
                        case when instr(utca_lev, '''') > 0 or instr(utca_lev, '"') > 0 or instr(utca_lev, ',') > 0 then
                          '"'||replace(substr(utca_lev, 1, utca_hossz - (REGEXP_COUNT(utca_lev, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(utca_lev, 1, utca_hossz)
		                end,                                      --levelezési cím utca
                        decode(levelezesi_h, null, to_char(levelezesi_r, 'YYYYMMDD'), to_char(levelezesi_h, 'YYYYMMDD')),  --levelezési cím hatálya (rendszerbe kerülése)
                        to_char(m054_pf_lev),                     --postafiókos levelezési cím irányító száma 
                        case when instr(telnev_pf_lev, '''') > 0 or instr(telnev_pf_lev, '"') > 0 or instr(telnev_pf_lev, ',') > 0 then
                          '"'||replace(substr(telnev_pf_lev, 1, telnev_hossz - (REGEXP_COUNT(telnev_pf_lev, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(telnev_pf_lev,1,telnev_hossz)
		                end,                                        -- postafiókos levelezési cím település neve
                        case when instr(pfiok_lev, '''') > 0 or instr(pfiok_lev, '"') > 0 or instr(pfiok_lev, ',') > 0 then
                          '"'||replace(substr(pfiok_lev, 1, pfiok_lev_hossz - (REGEXP_COUNT(pfiok_lev, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(pfiok_lev, 1, pfiok_lev_hossz)
		                end,                                           -- postafiók			
                        decode(lev_pf_r, null, '', to_char(lev_pf_r, 'YYYYMMDD')), --pf. cím hatálya (rendszerbe kerülése)
                        m040,                                                  --mûködés állapotkódja 
						to_char(m040k, 'YYYYMMDD'),                                                         --mûködési kód hatálya
                        decode(mukodv, null, '', to_char(mukodv,'YYYYMMDD')),             --mûködés vége
                        m025,                                   -- létszám kategória
                        decode(letszam_h, null, '', to_char(letszam_h, 'YYYYMMDD')), --létszám kategória besorolás dátuma			
                        m026,            --árbevétel kategória	
						decode(arbev_h, null, to_char(alakdat, 'YYYYMMDD'), to_char(arbev_h, 'YYYYMMDD')),   --árbevétel kategória hatálya 
						m009_szh,
						decode(alakdat, null, '', to_char(alakdat, 'YYYYMMDD')),            --alakulás dátuma
                        m0781,                                                          --admin szakág 2008
                        to_char(m0781_h, 'YYYYMMDD'),   --2008-as besorolási kód hatálya
                        m058_j,                        --janus TEÁOR
                        to_char(m0581_h, 'YYYYMMDD'),   --janus TEÁOR hatálya (azonos a stat TEÁOR hatályával)
                        decode(MP65, 'S9900', null, MP65),   --decode(m063,null,'90',m063)                            --ESA szektorkód
                        to_char(MP65_H, 'YYYYMMDD'),  --to_char(decode(m063_h,null,alakdat,m063_h),'YYYYMMDD')  --ESA szektorkód hatálya
                        decode(ueleszt, null, '', to_char(ueleszt,'YYYYMMDD')),  --újraélesztés hatálya
                        decode(m003_r, null, '', to_char(m003_r,'YYYYMMDD')),    --rendszerbe kerülés dátuma
                        to_char(datum, 'YYYYMMDD'),     --leválogatás dátuma
                        M0581_J,                         --statisztikai TEÁOR 2025. január 01-tõl janus
                        to_char(M0583_H, 'YYYYMMDD'),   --stathatály 2025
					    cegv,                           -- cégjegyzék szám
						to_char(cegv_h, 'YYYYMMDD'),     -- hatálya
						nvl(mvb39, '0'),                 -- IFRS nyilatkozat ha null, akkor legyen 0
						nvl(to_char(mvb39_h, 'YYYYMMDD'),case when to_char(alakdat,'YYYY') < '2016' then '20160101' else to_char(alakdat,'YYYYMMDD') end),    -- hatálya ha nincsen akkor az alakulás dátuma kivéve, ha az alakulás 2016.01.01-nél régebbi, akkor 2016.01.01
						null,                           --Országkód lesz másik táblából
                        nvl(to_char(LETSZAM), 'N/A'),
                        nvl(to_char(ARBEV), 'N/A'),
                        M0783, --'0111', --M0781, --M0783,                              --Adminisztratív TEÁOR 2025
                        to_char(M0783_H, 'YYYYMMDD'), --'20250101',--to_char(M0781_H, 'YYYYMMDD'),
                        M0583, --'0111', --M0581, --M0583,                          --Statisztikai TEÁOR 2025
                        to_char(M0583_H, 'YYYYMMDD'), --'20250101',--to_char(M0581_H, 'YYYYMMDD'),           
                        0,                           --0 = Statisztikai TEÁOR, 1 = nemzeti számlás TEÁOR
                        null,                          --E-mail cím lesz másik táblából
                        null,                           --E-mail cím kezdõ hatálya lesz másik táblából    
                        null,                           --Cégkapu lesz másik táblából
                        null,                           --Cégkapu kezdõ hatálya lesz másik táblából
                        -- rendszerbe kerülések a vizsgálatokhoz:	
						m003_r,                         -- törzsszám rendszerbe kerülése
                        m0491_r, --'YYYYMMDD'),
                        m005_szh_r, --'YYYYMMDD'),
                        nev_r, --'YYYYMMDD'),
                        rnev_r, --'YYYYMMDD'),
                        szekhely_r, --'YYYYMMDD'),
                        m040k_r, --'YYYYMMDD'),
                        m040v_r, --'YYYYMMDD'),
                        m0783_r, --'YYYYMMDD'),
                        m0583_r, --'YYYYMMDD'),
                        MP65_r, --'YYYYMMDD'),  --m063_r
                        arbev_r, --'YYYYMMDD'),
						levelezesi_r,
                        lev_pf_r, 
                        letszam_R,--Szilágyi Ádám 2022.10.19 _R _h helyett
						cegv_r,
						mvb39_r,
                        UELESZT_R--Szilágyi Ádám 2022.10.19.
                    into 
					    mnb_rec.torzsszam,            -- 8
                        mnb_rec.gfo,                  -- 3
                        mnb_rec.gfo_hataly,           --GFO hatály  8
                        mnb_rec.megyekod,             --székhely megyekódja,  2
                        mnb_rec.megyekod_hataly,      --a megyekód hatály dátuma  8
                        mnb_rec.nev,                  --név  250
                        mnb_rec.nev_h,                --név hatálya  8
                        mnb_rec.rnev,                 --rövid név  40
                        mnb_rec.RNEV_H,               --rövid név hatálya  8
                        mnb_rec.M054_SZH,             --székhely irányító szám  4
                        mnb_rec.telnev_szh,           --székhely	20
                        mnb_rec.utca_szh,             --székhely utca, házszám  80
                        mnb_rec.SZEKHELY_H,           --székhely cím hatálya 	8
                        mnb_rec.M054_LEV,	          --levelezési cím irányító száma  4
                        mnb_rec.telnev_lev,           --levelezési cím település név	20
                        mnb_rec.utca_lev,             --levelezési cím utca	   80
                        mnb_rec.LEVELEZESI_R,         --levelezési cím rendszerbe kerülése	8
                        mnb_rec.m054_pf_lev,          --postafiókos levelezési cím irányító száma   8
                        mnb_rec.telnev_pf_lev,        --postafiókos levelezési cím település neve   4
                        mnb_rec.pfiok_lev,            --postafiók    20
                        mnb_rec.leV_PF_R,             --pf. cím hatálya (rendszerbe kerülése)    8
                        mnb_rec.M040,                 --mûködés állapotkódja      
                        mnb_rec.m040k,                --mûködési állapot hatálya	8					
                        mnb_rec.mukodv,               --mûködés vége                8
                        mnb_rec.m025,                 --létszám kategória           2
                        mnb_rec.letszam_h,            --létszám kategória besorolás dátuma  8
                        mnb_rec.m026,                 --árbevétel kategória    1
						mnb_rec.arbev_h,              --árbevétel kategória hatálya    8
						mnb_rec.m009_szh,             --székhely település kódja        5
                        mnb_rec.alakdat,              --alakulás dátuma                8
                        mnb_rec.m0781,                --admin szakág 2008                 4
                        mnb_rec.m0781_h,              --2008-as besorolási kód hatálya    8
                        mnb_rec.m058_j,               --janus TEÁOR                        4
                        mnb_rec.m0581_j_h,            --janus TEÁOR hatálya (azonos a stat TEÁOR hatályával)   8
                        mnb_rec.MP65,        --m063        --ESA szektorkód                       2
                        mnb_rec.MP65_H,      --m063_h         --ESA szektorkód hatálya                8
                        mnb_rec.ueleszt,              --újraélesztés hatálya                  8
                        mnb_rec.m003_r,               --szervezet rendszerbe kerülése         8
                        mnb_rec.datum2,               --napi leválogatás dátuma                8
                        mnb_rec.statteaor,            --statisztikai TEÁOR                     4
                        mnb_rec.stathataly,           --statisztikai TEÁOR hatálya             8
						mnb_rec.cegjegyz,             --cégjegyzék szám
						mnb_rec.cegjegyz_h,           --cégjegyzékszám hatálya
						mnb_rec.mvb39,                --IFRS nyilatkozat
						mnb_rec.mvb39_h,          	  --IFRS hatálya
                        mnb_rec.ORSZ,                 --Országkód
                        mnb_rec.LETSZAM,              --Létszám
                        mnb_rec.ARBEV,                --Árbevétel
                        mnb_rec.M0783,                 --Adminisztratív TEÁOR 2025
                        mnb_rec.M0783_H,               --2025-ös besorolási kód hatálya
                        mnb_rec.M0583,                 --Statisztikai TEÁOR 2025
                        mnb_rec.M0583_H,               --2025-ös besorolási kód hatálya
                        mnb_rec.stat_szakag_típus,     --0 = Statisztikai TEÁOR, 1 = nemzeti számlás TEÁOR
                        mnb_rec.EMAIL,                 --E-mail cím
                        mnb_rec.EMAIL_H,               --E-mail cím hatály kezdete
                        mnb_rec.Cegkapu,               --Cégkapu
                        mnb_rec.Cegkapu_H,             --Cégkapu hatály kezdete
						-- rendszerbe kerülések a vizsgálatokhoz dátum típusúak:
                        mnb_rec.m003_r_date,
                        mnb_rec.m0491_r,
                        mnb_rec.m005_szh_r,
                        mnb_rec.nev_r,
                        mnb_rec.rnev_r,
						mnb_rec.szekhely_r,
                        mnb_rec.m040k_r,
                        mnb_rec.m040v_r,
                        mnb_rec.m0783_r,
                        mnb_rec.m0583_r,
                        mnb_rec.MP65_R,  --mnb_rec.m063_r
                        mnb_rec.arbev_r,
						mnb_rec.levelezesi_r_date,
                        mnb_rec.lev_pf_r_date,	
                        mnb_rec.letszam_h_date,
						mnb_rec.cegjegyz_r,
						mnb_rec.mvb39_r,
                        mnb_rec.UELESZT_R--Szilágyi Ádám 2022.10.19.
                    from 
                        vb.f003 
                    where m003=gszr_rec.m003;
                exception
                    when others then 
                        serr:=sqlcode;
                        insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ: '||to_char(gszr_rec.m003)||':'||datum_kar,' F003 select hiba');
                        commit;
						dbms_output.put_line('HIBA a ' || to_char(gszr_rec.m003) || ' törzsszámnál, amelynek üzenete: ' || sqlerrm || '.');
                        exit;
     			end;
				--település kód + cdv
                begin
                    select g.m009_szh||f.m009cdv into mnb_rec.m009_szh from f009_akt f,vb.f003 g where g.m003=gszr_rec.m003 and f.m009=g.m009_szh;
                exception
                    when no_data_found then                   --azon elképzelhetetlül (?) ritka esetekben, ha az avar kori települést nem találnánk meg az F009-ben:
                        serr:=sqlcode;
                        insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ: '||programneve||datum_kar||':'||to_char(mnb_rec.torzsszam)||' '||to_char(mnb_rec.m009_szh),' nincs cdv a településhez'||datum_kar);
                        commit;
                        mnb_rec.m009_szh:=mnb_rec.m009_szh||'X';
                end;                               --székhely település kód+cdv 5 jegyen	
                eddig:=11;				
                
                 begin
                    SELECT EELERHETOSEG, TO_CHAR(HATALY, 'YYYYMMDD') HATALY INTO mnb_rec.EMAIL, mnb_rec.EMAIL_H  from 
                    (select EELERHETOSEG, HATALY from VB.F003_EELERHETOSEG 
                    where M003 = gszr_rec.m003 and MVB42 in ('10', '11') 
                    order by M003, HATALY desc, MVB42, DATUM_R desc 
                    offset 0 row fetch first 1 rows only);
                    
                    if mnb_rec.EMAIL_H < mnb_rec.ALAKDAT then
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám e-mail címének hatálya (' || mnb_rec.EMAIL_H || ') az alakulás dátumára változott (' || mnb_rec.ALAKDAT || '), mert attól korábbi volt.');
                        mnb_rec.EMAIL_H := mnb_rec.ALAKDAT;
                    end if;
                     
                exception
                    when no_data_found then
                        mnb_rec.EMAIL := null;
                        mnb_rec.EMAIL_H := null;
                         
                end;
                
                
              
                begin
                    SELECT replace(EELERHETOSEG, 'hivatali', 'ceg'), TO_CHAR(HATALY, 'YYYYMMDD') HATALY INTO mnb_rec.Cegkapu, mnb_rec.Cegkapu_H  from 
                    (select EELERHETOSEG, HATALY from VB.F003_EELERHETOSEG 
                    where M003 = gszr_rec.m003 and MVB42 = '60' 
                    order by M003, HATALY desc, MVB42, DATUM_R desc 
                    offset 0 row fetch first 1 rows only);
                    
                    if substr(mnb_rec.Cegkapu, 1, 8) != mnb_rec.torzsszam then
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszámú cég cégkapuja (' || mnb_rec.Cegkapu || ') megváltozott a törzsszám#cegkapu formátumra.');
                        mnb_rec.Cegkapu := mnb_rec.torzsszam || '#cegkapu';
                    end if;
                    
                    if mnb_rec.Cegkapu_H < mnb_rec.ALAKDAT then
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám cégkapujának hatálya (' || mnb_rec.Cegkapu_H || ') az alakulás dátumára változott (' || mnb_rec.ALAKDAT || '), mert attól korábbi volt.');
                        mnb_rec.Cegkapu_H := mnb_rec.ALAKDAT;
                    end if;
                     
                exception
                    when no_data_found then
                        mnb_rec.Cegkapu := null;
                        mnb_rec.Cegkapu_H := null;
                         
                end;
                
                
                begin
                    SELECT ORSZ into mnb_rec.ORSZ
                      FROM (SELECT distinct M003, ORSZ,
                                   DATUM_R,
                                   rank() over (partition by M003 order by DATUM_R desc) rnk
                              FROM VB_CEG.VB_APEH_CIM where M003 = gszr_rec.m003)
                     WHERE rnk = 1;
                     
                     
                     if(mnb_rec.ORSZ = 'HU' and mnb_rec.megyekod > 20) then --M005_SZH
                        mnb_rec.ORSZ := 'Z8'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám országkódja HU-ról Z8 lett, mert a megyekód (M005_SZH = ' || mnb_rec.megyekod || ') > 20.');
                     end if;
                     
                     if(mnb_rec.ORSZ = 'XX') then --M005_SZH
                        mnb_rec.ORSZ := 'Z8'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám országkódja XX-rõl Z8 lett, mert XX értéket az MNB nem tud fogadni.');
                     end if;
                     
                     --Amennyiben üres országkóddal van bent egy cég a VB_CEG.VB_APEH_CIM adatbázis táblában
                     if(mnb_rec.ORSZ is null and mnb_rec.megyekod < 21) then --M005_SZH
                        mnb_rec.ORSZ := 'HU'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám országkódja üresrõl HU lett, mert a megyekód (M005_SZH = ' || mnb_rec.megyekod || ') < 21.');
                     end if;
                     
                     if(mnb_rec.ORSZ is null and mnb_rec.megyekod > 20) then --M005_SZH
                        mnb_rec.ORSZ := 'Z8'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám országkódja üresrõl Z8 lett, mert a megyekód (M005_SZH = ' || mnb_rec.megyekod || ') > 20.');
                     end if;
                     
                exception
                    when no_data_found then
                    
                         if(mnb_rec.ORSZ is null and mnb_rec.megyekod < 21) then --M005_SZH
                            mnb_rec.ORSZ := 'HU'; 
                            dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám országkódja üresrõl HU lett, mert a megyekód (M005_SZH = ' || mnb_rec.megyekod || ') < 21.');
                         end if;
                     
                         if(mnb_rec.ORSZ is null and mnb_rec.megyekod > 20) then --M005_SZH
                            mnb_rec.ORSZ := 'Z8'; 
                            dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám országkódja üresrõl Z8 lett, mert a megyekód (M005_SZH = ' || mnb_rec.megyekod || ') > 20.');
                         end if;
                         
                end;
                eddig:=110;
                
                begin
                    if(mnb_rec.MP65 is null ) then 
                        mnb_rec.MP65_H := null; 
                        --dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám MP65_H értéke üres lett.');
                     end if;
                end;
                
                begin
                    if (ALAKDAT_MODSZAM > 0) then
                        select M003 into M003_ALAKDAT_CHANGED from vb.f003_hist3pr where datum > utolso_futas and alakdat != alakdat_u and M003 = gszr_rec.m003;  
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám alakulás dátuma megváltozott a következõre: ' || to_char(mnb_rec.alakdat) || '.');
                    end if;     
                    
                    exception
                        when no_data_found then
                        null;
                             
                end;
                
-- A GSZR-rekord kigyûjtésének vége.
-- Van-e változás a GSZR-rekordban vagy az F003_m0582-ben?		
                nsz_db := 0;
                begin
                        select count(*) into nsz_db from VB.F003_M0582 g
                        where g.M003 = gszr_rec.m003 and g.M0582_R = (select max(M0582_R) from 
						VB.F003_M0582 where M003 = gszr_rec.m003 and M0582_HV is null);
                exception
                    when others then
                        serr:=sqlcode; 
                        errmsg:=substr(sqlerrm,1,40);
                        insert into  vb.vb_uzenet (number1,text1,text2) 
                              values(serr,'mnb napi adatküldõ:F003_m0582 lekérdezés'||datum_kar||':'||to_char(mnb_rec.torzsszam),errmsg);
                        commit; 
                end;
				eddig:=12;
                if nsz_db = 1 then
                 -- csak egy nsz-rekord van utolsó
                    select m0582,m0582_h,m0582_hv,m0582_r, kuldes_vege into w_statteaor,w_stathataly,w_hatalyvege,w_stat_r, w_kuldes_vege from 
					vb.f003_m0582 g
                    where m003 = gszr_rec.m003 and m0582_r=(select max(m0582_r) from  vb.f003_m0582 
					                                      where m003 = gszr_rec.m003);
                elsif nsz_db > 1 then
                 --egy nap több változás volt. Ezek közül csak egy lehet nyitott: nem lezárt, az kell
                    select m0582,m0582_h,m0582_hv,m0582_r, kuldes_vege  into w_statteaor,w_stathataly,w_hatalyvege,w_stat_r, w_kuldes_vege from 
					vb.f003_m0582 g
                    where m003 = gszr_rec.m003 and m0582_r=(select max(m0582_r) from vb.f003_m0582
					                                      where m003 = gszr_rec.m003)
                    and m0582_hv is null;
                end if;
			    eddig:=13;
--a hatályokat kalkulálgatjuk:			  
                if nsz_db != 0 then
			if mnb_rec.statteaor = w_statteaor then
                    		dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám esetén M0581_J = M0582 = ' || mnb_rec.statteaor || ', de a hatály M0583_H = ' || mnb_rec.stathataly || ' az NSZ TEÁOR hatályra M0582_H = ' || to_char(w_stathataly, 'YYYYMMDD') || ' változott.');
                	else
                    		dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' törzsszám M0581_J = ' || mnb_rec.statteaor || ' értéke és M0583_H = ' || mnb_rec.stathataly || ' hatálya az NSZ TEÁOR kódjára M0582 = ' || w_statteaor || ' és annak hatályára M0582_H = ' || to_char(w_stathataly, 'YYYYMMDD') || ' változott.');
                	end if;
                        mnb_rec.statteaor := w_statteaor;
                        mnb_rec.stathataly := to_char(w_stathataly,'YYYYMMDD');
                    /*    mnb_rec.M0583 := w_statteaor;
                        mnb_rec.M0583_H := to_char(w_stathataly,'YYYYMMDD');
                        mnb_rec.stat_szakag_típus := '1';*/
						eddig:=17;
                end if; --nsz_db!=0
                 --létszám- árbevétel kategória, és ESA kód
				--dbms_output.put_line('nsz-TEÁOR kész');
                w_m025:='  ';
                w_m026:=' ';
              --  w_m063:='  ';
                eddig:=18;				
                --dbms_output.put_line('ESA kód');            
                begin
                    select m025 into w_m025 from vb.f003_hist2 where m003=mnb_rec.torzsszam and datum=(select max(datum) from vb.f003_hist2 where m003=mnb_rec.torzsszam)
                    and rownum=1;
                exception
                    when no_data_found then
                       null;
                end; 
                eddig:=19;				
                --dbms_output.put_line('Létszám kategória');                  
                begin
                    select m026 into w_m026 from vb.f003_hist2 where m003=mnb_rec.torzsszam and datum=(select max(datum) from vb.f003_hist2 where m003=mnb_rec.torzsszam)
                    and rownum=1;
                exception
                    when no_data_found then
                        null;
                end;
                eddig:=20;				
                mehet:=false;
--megnézzük, miért került a kurzorba a törzsszám				
                begin
                    select count(*) into kulondb from vb_rep.vb_app_init where 
					param_nev='m003' 
					--and  program=programneve
					and m003=gszr_rec.m003  and to_date(param_dtol,'YYYY-MM-DD hh24:mi:ss')>=utolso_futas;
                exception
                    when others then
					    serr:=sqlcode;
                        kulondb:=0;
						--dbms_output.put_line('különdb exception :'||to_char(serr));
                end;
				--dbms_output.put_line(to_char(gszr_rec.m003)||':különdb:'||to_char(kulondb));
				eddig:=21;
				--dbms_output.put_line('van-e kulondb:'||to_char(kulondb));
                if kulondb!=0 or 
				   ((w_stathataly >= utolso_futas and w_statteaor!=mnb_rec.statteaor and w_kuldes_vege is null) or --a nemzeti számlás TEÁOR az utolsó futás óta került be
                   (w_kuldes_vege>= utolso_futas)) then
                      mehet:=true;
					  EDDIG:=22;
                  --dbms_output.put_line('mehet1');                         
                else   
                  --dbms_output.put_line('mehet2');                    
                 -- nem árbevétel, létszám kat., vagy ESA kód miatt került a kurzorba
				    eddig:=23;
                    begin      
                      --és statisztika gyûjtése					
						if mnb_rec.m003_r_date  >= utolso_futas then 
							m003_r_db:=m003_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('m003_r');     
						if mnb_rec.m0491_r      >= utolso_futas then 
							m0491_r_db:=m0491_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('m0491_r'); 
						if mnb_rec.m005_szh_r   >= utolso_futas then 
							m005_szh_db:=m005_szh_db+1;
							mehet:=true; end if;
						--dbms_output.put_line('m005_r'); 
						if mnb_rec.nev_r        >= utolso_futas then 
							nev_r_db:=nev_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('nev_r'); 
						if mnb_rec.rnev_r       >= utolso_futas then 
							rnev_r_db:=rnev_r_db+1;
							mehet:=true; 
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_rnev := m003_r_db_for_rnev + 1; 
                            end if;
						end if;
						--dbms_output.put_line('rnev_r'); 
						if mnb_rec.szekhely_r   >= utolso_futas then 
							szekhely_r_db:=szekhely_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('szekhely_r'); 
						if mnb_rec.levelezesi_r_date >= utolso_futas then 
							levelezesi_r_db:=levelezesi_r_db+1;
							mehet:=true; 
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_levelezesi := m003_r_db_for_levelezesi + 1; 
                            end if;
						end if;
						--dbms_output.put_line('levelezesi_r'); 
						if mnb_rec.LEV_PF_R_date     >= utolso_futas then 
							LEV_PF_R_db:=LEV_PF_R_db+1;
							mehet:=true; 
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_lev_pf := m003_r_db_for_lev_pf + 1; 
                            end if;
						end if;
						--dbms_output.put_line('lev_pf_r'); 
						if mnb_rec.m040k_r      >= utolso_futas then 
							m040k_r_db:=m040k_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('m040k_r');
						if mnb_rec.m040v_r      >= utolso_futas then 
							m040v_r_db:=m040v_r_db+1;
							mehet:=true; 
						end if;
						--dbms_output.put_line('m040v_r'); 
						if mnb_rec.m0783_r      >= utolso_futas then 
							m0783_r_db := m0783_r_db + 1;
							mehet := true; 
						end if;
						--dbms_output.put_line('m0783_r'); 
						if mnb_rec.letszam_h_date>= utolso_futas then
							letszam_h_db:=letszam_h_db+1;
						end if;	
						--dbms_output.put_line('letszam'); 
						if mnb_rec.arbev_r>=utolso_futas then
							arbev_r_db:=arbev_r_db+1;
						end if;
						--dbms_output.put_line('ARBEV');
						if mnb_rec.m0583_r >= utolso_futas then
							m0583_r_db := m0583_r_db + 1;
						end if;
						--dbms_output.put_line('StatTEÁOR');
						if mnb_rec.MP65_r >= utolso_futas then  --m063_r
							MP65_r_db := MP65_r_db + 1; --m063_r_db:=m063_r_db+1;
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_MP65 := m003_r_db_for_MP65 + 1;  --m003_r_db_for_m063 := m003_r_db_for_m063 + 1; 
                            end if;
						end if;	
						--dbms_output.put_line('ESA');
						if mnb_rec.ueleszt_R>=utolso_futas then--Szilágyi Ádám 2022.10.19. _R
							ueleszt_db:=ueleszt_db+1;
						end if;
						--dbms_output.put_line('cégjegyzék szám);
						if mnb_rec.cegjegyz_r>=utolso_futas then
							cegv_db:=cegv_db+1;
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_cegv := m003_r_db_for_cegv + 1; 
                            end if;
						end if;
						--dbms_output.put_line('IFRS');
						if mnb_rec.mvb39_r>=utolso_futas then
							ifrs_db:=ifrs_db+1; 
						end if;
						--dbms_output.put_line('ueleszt');
	/*                  if mnb_rec.m0583_r    >= to_char(utolso_futas,'YYYYMMDD') then mehet:=true; end if;
						dbms_output.put_line('m0583_r'); */
						--dbms_output.put_line(mnb_rec.ueleszt);
					exception when others then
					   dbms_output.put_line(to_char(sqlcode));
					end;
					eddig:=24;
					if nvl(TO_DATE(mnb_rec.ueleszt,'yyyymmdd'),sysdate)  >= utolso_futas then mehet:=true; end if;
                    --dbms_output.put_line('ueleszt'); 
					eddig:=25;
                    megszunt:=false; 
                    if (mnb_rec.m040 not in ('0','9') or substr(mnb_rec.m040k,1,4)=to_char(sysdate,'YYYY'))  -- élõ, vagy az adott naptári évben szûnt meg --substr(mnb_rec.m040k,1,1) volt substr(mnb_rec.m040k,1,4) helyett, de így csak az évszám elsõ jegye volt hasonlítva YYYY-al
                              or 
                            ( mnb_rec.m040 in ('0','9') and mnb_rec.m040k_r>=utolso_futas) or kulondb>0  then 
                            mehet:=true;
                            --dbms_output.put_line(mnb_rec.torzsszam||' mehet');	
	                else 
                             mehet:=false;
                              megszunt:=true;
                              --dbms_output.put_line(mnb_rec.torzsszam||' megpusztult,'||mnb_rec.m040k||' nem mehet');	
							 dbms_output.put_line('A következõ törzsszám kihagyva, mert korábbi évben (' || substr(mnb_rec.m040k, 1, 4) || '-' || substr(mnb_rec.m040k, 5, 2) || '-' ||  substr(mnb_rec.m040k, 7, 2) || ') szûnt meg: ' || mnb_rec.torzsszam || '.');	
                    end if;--vagy, ha a megszûnési információ csak most került a regiszterbe.
					eddig:=26;
             
                    if not mehet and not megszunt then
                       
						eddig:=27;
                        if mnb_rec.letszam_h_date    >= utolso_futas and mnb_rec.m025!=nvl(w_m025,'00') then 
                            mehet:=true; 
							--dbms_output.put_line('letszam_h miatt mehet');							 
                        elsif mnb_rec.m025=nvl(w_m025,'00') then     
                            dbms_output.put_line('Nem változott:'||to_char(mnb_rec.torzsszam)||' m025:'||w_m025||'='||mnb_rec.m025);                       
                            nem_valtozott:=nem_valtozott+1;
                        end if;  
                        eddig:=28;						
                        if mnb_rec.arbev_r      >= utolso_futas and mnb_rec.m026!=nvl(w_m026,'0') then
                            mehet:=true; 
							--dbms_output.put_line('arbev_r miatt mehet');	
                        elsif mnb_rec.m026=nvl(w_m026,'0') then                       
                            dbms_output.put_line('Nem változott:'||to_char(mnb_rec.torzsszam)||' m026:'||w_m026||'='||mnb_rec.m026);  
                            nem_valtozott:=nem_valtozott+1;                                   
                        end if; 
						eddig:=29;
						--2019.09.06.  Levizsgáljuk, hogy megváltozott-e az alakulás dátuma utólag, hogy ha más nem változott, akkor is benne maradjon a 
                        --           leválogatásban
						w_alakdat:=null;
						begin
							select alakdat_u into w_alakdat from vb.f003_hist3pr where
							m003=mnb_rec.ksh_torzsszam and datum>utolso_futas and alakdat!=alakdat_u;
						exception when no_data_found then
						     null;
						end;
						if w_alakdat is not null then 
						--megváltozott az alakulás dátuma, mindenhogyan át kell adni!
						    mehet:=true;
						end if;	
                    end if;
                end if;
                --dbms_output.put_line('m026 a hist2-bõl');                   
                --dbms_output.put_line('Határellenõrzés jön');                        
                 ---  Küldhetõ lenne, de valamelyik dátum hibás, ezért mégsem küldhetõ
                if  not to_date(mnb_rec.gfo_hataly,'YYYYMMDD')    between alsohatar and felsohatar  then 
                      mehet:=false; 
                      dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':m0491_h:'||mnb_rec.gfo_hataly); 
                end if;
				eddig:=30;
                 --dbms_output.put_line('m0491_h'); 
                if not to_date(mnb_rec.megyekod_hataly,'YYYYMMDD')  between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                      mehet:=false;
                      dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':m005_szh_h:'||mnb_rec.megyekod_hataly); 
                end if;
				eddig:=31;
                 --dbms_output.put_line('m005_szh_h'); 
                if not to_date(mnb_rec.nev_h,'YYYYMMDD')        between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                      mehet:=false;                 
                      dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':nev_h:'||mnb_rec.nev_h ); 
                 end if;
				 eddig:=32;
                 begin
				 --dbms_output.put_line('nev_h'); 
                if not nvl(to_date(mnb_rec.RNEV_H,'YYYYMMDD') ,sysdate)   between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                      mehet:=false;                   
                      dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':rnev_h:'||mnb_rec.RNEV_H); 
                 end if;
				 exception when others then
				     dbms_output.put_line(to_char( mnb_rec.torzsszam)||':nev_h:'); 
				 end;
				 eddig:=33;
                 --dbms_output.put_line('rnev_h'); 
                if not to_date(mnb_rec.SZEKHELY_H,'YYYYMMDD')    between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527')  then 
                       mehet:=false;  
                       dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':SZEKHELY_H:'||mnb_rec.SZEKHELY_H); 
                 end if;
				 eddig:=34;
                 --dbms_output.put_line('szekhely_h'); 
                if not nvl(mnb_rec.LEVELEZESI_R_date,sysdate)  between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                       mehet:=false;  
                       dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':LEVELEZESI_R:'||mnb_rec.LEVELEZESI_R); 
                 end if;
				 eddig:=35;
                 --dbms_output.put_line('levelezesi_r'); 
                if not nvl(mnb_rec.leV_PF_R_date,sysdate)   between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                       mehet:=false;  
                       dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':leV_PF_R :'||mnb_rec.leV_PF_R); 
                 end if;
				 eddig:=36;
                 --dbms_output.put_line('lev_pf_r'); 
                if not nvl(to_date(mnb_rec.mukodv,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':mukodv:'||mnb_rec.mukodv); 
                 end if;
				 eddig:=37;
                 --dbms_output.put_line('mukodv'); 
                if not nvl(to_date(mnb_rec.letszam_h,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':letszam_h:'||mnb_rec.letszam_h); 
                 end if;
				 eddig:=38;
                 --dbms_output.put_line('letszam_h'); 
                if not nvl(to_date(mnb_rec.arbev_h,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':arbev_h:'||mnb_rec.arbev_h); 
                 end if;
				 eddig:=39;
                 --dbms_output.put_line('arbev_h'); 
                if not to_date(mnb_rec.alakdat,'YYYYMMDD')  between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527')  then 
                       mehet:=false;                   
                       dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':alakdat:'||mnb_rec.alakdat); 
				elsif mnb_rec.torzsszam in ('15302724','15736527')  then 
				       mnb_rec.alakdat:='19830101';
                end if;
				 eddig:=40;
                 --dbms_output.put_line('alakdat'); 
                if not to_date(mnb_rec.m0781_h,'YYYYMMDD') between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':m0781_h:'||mnb_rec.m0781_h); 
                 end if;
				 eddig:=41;
                --dbms_output.put_line('m0781_h'); 
				--dbms_output.put_line('stathatály:'||mnb_rec.stathataly);
                if not mnb_rec.stathataly  between to_char(alsohatar,'YYYYMMDD') and to_char(felsohatar,'YYYYMMDD') then 
                       mehet:=false;                   
                       dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||':m0581_h:'||mnb_rec.stathataly); 
                 end if;
				 eddig:=42;
                 --dbms_output.put_line('m0581_h'); 
                if not nvl(to_date(mnb_rec.MP65_h,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then   --m063_h
                       mehet:=false;                   
                       dbms_output.put_line('Hibás dátum: '||to_char(mnb_rec.torzsszam) || ' :mp65_h: ' || mnb_rec.MP65_h);  --mnb_rec.m063_h
                 end if;
				 eddig:=43;
                 --dbms_output.put_line('m063_h'); 
                if not nvl(to_date(mnb_rec.ueleszt,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hibás dátum: '||to_char(mnb_rec.torzsszam)||'ueleszt: '||mnb_rec.ueleszt); 
                 end if;
				 eddig:=44;
                 --dbms_output.put_line('ueleszt'); 
                if not to_date(mnb_rec.m003_r,'YYYYMMDD') between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hibás dátum:'||to_char(mnb_rec.torzsszam)||'m003_r :'||mnb_rec.m003_r); 
                 end if;
				 eddig:=45;
                 --dbms_output.put_line('m003_r'); 
                 --dbms_output.put_line('Határellenõrzés vége');      
           -- $if $$debug_on $then
			  -- dbms_output.put_line(mnb_rec.torzsszam||' '||case when mehet then 'mehet' else 'nem mehet' end);
			--$end
            --dbms_output.put_line('Dátumhibák ellenõrizve');                  
            if mehet then
                sor:=mnb_rec.q01||','||                           -- adatgyûjtés kódja
                    mnb_rec.datum1||','||                         -- vonatkozási idõ 8 hosszon
                    mnb_rec.ksh_torzsszam||','||                  -- KSH törzsszáma
                    mnb_rec.kitoltes_datum||','||                 -- kitöltés dátuma 8 hosszon
                    mnb_rec.kshtorzs||                            -- 1 karakter fixen: E, táblakód: @KSHTORZS
                    mnb_rec.rekordsorszam||','||                  -- sorszám 7 karakteren elõnullázva  a kurzor rekordszámából levonva az eddig átlépett rekordok számát
                    mnb_rec.torzsszam||','||                      -- törzsszam
                    mnb_rec.gfo||','||                            -- GFO
                    mnb_rec.gfo_hataly||','||                     -- GFO hatály
                    mnb_rec.megyekod||','||                       --székhely megyekódja, 
                    mnb_rec.megyekod_hataly||','||                --a megyekód hatály dátuma
                    mnb_rec.nev||','||                            --név
                    mnb_rec.nev_h||','||                          --név hatálya
                    mnb_rec.rnev||','||                           --rövid név
                    mnb_rec.RNEV_H||','||                         --rövid név hatálya
                    mnb_rec.M054_SZH||','||                       --székhely irányító szám
                    mnb_rec.telnev_szh||','||                     --székhely	
                    mnb_rec.utca_szh||','||                       --székhely utca, házszám
                    mnb_rec.SZEKHELY_H||','||                     --székhely cím hatálya 	
                    mnb_rec.M054_LEV||','||	                      --levelezési cím irányító száma
                    mnb_rec.telnev_lev||','||                     --levelezési cím település név	
                    mnb_rec.utca_lev||','||             --levelezési cím utca	
                    mnb_rec.LEVELEZESI_R||','||         --levelezési cím rendszerbe kerülése	
                    mnb_rec.m054_pf_lev||','||          --postafiókos levelezési cím irányító száma 
                    mnb_rec.telnev_pf_lev||','||        --postafiókos levelezési cím település neve
                    mnb_rec.pfiok_lev||','||            --postafiók
                    mnb_rec.leV_PF_R||','||             --pf. cím hatálya (rendszerbe kerülése)
                    mnb_rec.M040||','||                 --mûködés állapotkódja átkódolva: 0,9->0, egyébként 1               
                    mnb_rec.m040k||','||                --állapotkód hatálya
                    mnb_rec.m025||','||                 --létszám kategória
                    mnb_rec.letszam_h||','||            --létszám kategória besorolás dátuma
                    mnb_rec.m026||','||                 --árbevétel kategória
                    mnb_rec.arbev_h||','||              --árbevétel kategória hatálya
                    mnb_rec.m009_szh||','||             --székhely település kód+cdv 5 jegyen					
                    mnb_rec.alakdat||','||              --alakulás dátuma
                    mnb_rec.m0781||','||                --admin szakág 2008
                    mnb_rec.m0781_h||','||              --2008-as besorolási kód hatálya
                    mnb_rec.m058_j||','||               --janus TEÁOR
                    mnb_rec.m0581_j_h||','||            --janus TEÁOR hatálya (azonos a stat TEÁOR hatályával)
                    mnb_rec.MP65||','||                 --ESA szektorkód   --mnb_rec.m063
                    mnb_rec.MP65_H||','||               --ESA szektorkód hatálya   -mnb_rec.m063_h
                    mnb_rec.ueleszt ||','||              --újraélesztés hatálya
                    mnb_rec.m003_r||','||               --szervezet rendszerbe kerülése
                    mnb_rec.datum2||','||               --napi leválogatás dátuma
                    mnb_rec.statteaor||','||            --statisztikai TEÁOR
                    mnb_rec.stathataly||','||           --statisztikai TEÁOR hatálya		
                    mnb_rec.cegjegyz||','||             --cégjegyzék szám
					mnb_rec.cegjegyz_h||','||           --cégjegyzékszám hatálya
					mnb_rec.mvb39||','||                --IFRS nyilatkozat
					mnb_rec.mvb39_h||','||              --IFRS nyilatkozat hatálya
                    mnb_rec.ORSZ || ',' ||              --Országkód
                    mnb_rec.LETSZAM || ',' ||           --Létszám
                    mnb_rec.ARBEV || ',' ||             --Árbevétel 
                    mnb_rec.M0783 || ',' ||             --Adminisztratív TEÁOR 2025
                    mnb_rec.M0783_H || ',' ||           --2025-ös besorolási kód hatálya
                    mnb_rec.M0583 || ',' ||             --Statisztikai TEÁOR 2025
                    mnb_rec.M0583_H || ',' ||           --2025-ös besorolási kód hatálya
                    mnb_rec.stat_szakag_típus || ',' || --0 = Statisztikai TEÁOR, 1 = nemzeti számlás TEÁOR
                    mnb_rec.EMAIL || ',' ||             --E-mail cím
                    mnb_rec.EMAIL_H || ',' ||           --E-mail cím hatály kezdete
                    mnb_rec.Cegkapu || ',' ||           --Cégkapu
                    mnb_rec.Cegkapu_H;                  --Cégkapu hatály kezdete;                      
                    
                    eddig:=46;					
               --táblába szúrjuk a küldendõ rekordot
			   $if $$debug_on $then
                    null;
                   --dbms_output.put_line('insert '||mnb_rec.torzsszam||mnb_rec.stathataly||mnb_rec.rekordsorszam);
               $end
                   begin
                        insert into 
						$if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end (kod,filename,sorszam,rekord)
                        values ('Q01',q01mellekletnev,rekordsorszam,sor);
                        --levalogatva:=levalogatva+1;
                   exception
                        when others then
                            serr:=sqlcode; 
                            errmsg:=substr(sqlerrm,1,40);
                            insert into  vb.vb_uzenet (number1,text1,text2) 
                                                values(serr,'mnb napi adatküldõ:'||programneve||datum_kar||':'||to_char(mnb_rec.torzsszam),errmsg);
                            commit; 
                            kihagyott:=kihagyott+1;                        
                    end;
					eddig:=47;
				rekordsorszam:=rekordsorszam+1;
            else
			    eddig:=48;
                kihagyott:=kihagyott+1;    
            end if;
			eddig:=49;
            --levalogatva:=sorok+mnb_rec.rn-kihagyott;    
            if mod(rekordsorszam+kihagyott,100)=0 then
                   begin
				   $if $$debug_on $then
                       vb.mod_szam_tolt('K865', tablaneve, sorok, 'MNB napi változáslista küldése, debug mód ', programneve || verzio, datum, 'M');     
                   $else
				       vb.mod_szam_tolt('K865', tablaneve, sorok, 'MNB napi változáslista küldése', programneve || verzio, datum, 'M');
				   $end
                   exception when others then
                       serr:=sqlcode;                   
                       insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ: ','mod_szam_tolt a hiba');
                   end;
                   commit;
            end if;
            eddig:=50;			
			fetch gszr_cur into gszr_rec;
        end loop;
    end if;           
    close gszr_cur;
    
        commit;
        select count(*) into levalogatva_1 from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where kod='Q01'
		and substr(rekord,5,8)=to_char(sysdate,'YYYYMMDD');-- and substr(rekord, 32, 1) != 'N';--Szilágyi Ádám 2022.10.21.
		eddig:=51;
        if(levalogatva_1 != 0) then
            dbms_output.put_line('Most leválogatott Q01-es sorok: ' || levalogatva_1);
       -- else 
       --     dbms_output.put_line('A mai nap folyamán nem került Q01-es adat leválogatásra.');
        end if;
		if puffersorok>0 then 
		    dbms_output.put_line('Puffer sorok száma:  '||puffersorok);
        end if;		
        if(kihagyott > 0) then 
            dbms_output.put_line('Kihagyott sorok: ' || kihagyott || ', amelyekbõl nem változott: ' || nem_valtozott || '.');
        end if;
        --dbms_output.put_line('ebbõl: nem változott:'||nem_valtozott);  
		
		--Statisztikák:
		dbms_output.put_line('');
    	select count(*) into osszesq01 from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where kod='Q01';
		--dbms_output.put_line('A táblában található Q01-es cégek száma: ' || to_char(osszesq01) || '.');
        dbms_output.put_line('---');
        --dbms_output.new_line;
        if(levalogatva_1 != 0) then
            dbms_output.put_line('Átfedõ statisztikák, amelyekbõl a második szám az érintett új törzsszámok számával csökkentett szám: ');
            --dbms_output.put_line('A második szám az új törzsszámok nélküli változások száma');
            --dbms_output.put_line('');
            dbms_output.new_line;
            if m003_r_db>0 then 
                dbms_output.put_line('Új törzsszám (M003): ' ||to_char(m003_r_db) || '.');
            end if;
            if m0491_r_db>0 then 
                dbms_output.put_line('GFO változás (M0491): ' ||to_char(m0491_r_db)|| ' : ' ||to_char(m0491_r_db-least(m003_r_db,m0491_r_db)) || '.');		
            end if;
            if m005_szh_db>0 then 
                dbms_output.put_line('Megyekód változás (M005_SZH): '||to_char(m005_szh_db)||' : '||to_char(m005_szh_db-least(m003_r_db,m005_szh_db)) || '.');				
            end if;	
            if nev_r_db>0 then 
                dbms_output.put_line('Név változás (NEV): '||to_char(nev_r_db)||' : '||to_char(nev_r_db-least(m003_r_db,nev_r_db)) || '.');		
            end if;
            if rnev_r_db>0 and m003_r_db_for_rnev = 0 then 
                dbms_output.put_line('Rövid név változás (RNEV): ' || to_char(rnev_r_db) || '.');-- ||' : '||to_char(rnev_r_db-least(m003_r_db,rnev_r_db)) || '.');		
            elsif rnev_r_db>0 and m003_r_db_for_rnev > 0 then
                dbms_output.put_line('Rövid név változás (RNEV): ' || to_char(rnev_r_db) || ' : ' || to_char(rnev_r_db - m003_r_db_for_rnev) || '.');		
            end if;
            if szekhely_r_db>0 then 
                dbms_output.put_line('Székhely változás (SZEKHELY): '||to_char(szekhely_r_db)||' : '||to_char(szekhely_r_db-least(m003_r_db,szekhely_r_db)) || '.');		
            end if;
            if levelezesi_r_db>0 and m003_r_db_for_levelezesi = 0 then 
                dbms_output.put_line('Levelezési cím változás (LEVELEZESI): ' || to_char(levelezesi_r_db) || '.');--||' : '||to_char(levelezesi_r_db-least(m003_r_db,levelezesi_r_db)) || '.');
            elsif levelezesi_r_db>0 and m003_r_db_for_levelezesi > 0 then
                dbms_output.put_line('Levelezési cím változás (LEVELEZESI): ' || to_char(levelezesi_r_db) || ' : ' || to_char(levelezesi_r_db - m003_r_db_for_levelezesi) || '.');
            end if;
            if LEV_PF_R_db>0 and m003_r_db_for_lev_pf = 0 then 
                dbms_output.put_line('Postafiók változás (LEV_PF): ' || to_char(lev_pf_r_db) || '.');-- ||' : '||to_char(lev_pf_r_db-least(m003_r_db,lev_pf_r_db)) || '.');		
            elsif LEV_PF_R_db>0 and m003_r_db_for_lev_pf > 0 then
                dbms_output.put_line('Postafiók változás (LEV_PF): ' || to_char(lev_pf_r_db) || ' : ' || to_char(lev_pf_r_db - m003_r_db_for_lev_pf) || '.');
            end if;
            if m040k_r_db>0 then 
                dbms_output.put_line('Állapotkód változás (M040K): '||to_char(m040k_r_db)||' : '||to_char(m040k_r_db-least(m003_r_db,m040k_r_db)) || '.');				
            end if;
            if m040v_r_db>0 then 
                dbms_output.put_line('Állapotkód vége (M040V): ' || to_char(m040v_r_db) || '.');-- || ' : ' || to_char(m040v_r_db-least(m003_r_db,m040v_r_db)) || '.');
            end if;
            if m0783_r_db >0 then 
                dbms_output.put_line('Adminisztratív TEÁOR változás (M0783): '||to_char(m0783_r_db)||' : '||to_char(m0783_r_db-least(m003_r_db, m0783_r_db)) || '.');
            end if;
            if letszam_h_db>0 then
                dbms_output.put_line('Létszám kategória változás (LETSZAM): '||to_char(letszam_h_db)||' : '||to_char(letszam_h_db-m003_r_db) || '.');
            end if;	
            if arbev_r_db>0 then
                dbms_output.put_line('Árbevétel-kategória változás (ARBEV): '||to_char(arbev_r_db)||' : '||to_char(arbev_r_db-least(m003_r_db,arbev_r_db)) || '.');		
            end if;
            if m0583_r_db >0 then
                dbms_output.put_line('Statisztikai TEÁOR változás (M0583): '||to_char(m0583_r_db)||' : '||to_char(m0583_r_db-least(m003_r_db, m0583_r_db)) || '.');		
            end if;
            /*if m063_r_db>0 and m003_r_db_for_m063 = 0 then
                dbms_output.put_line('ESA szektorkód változás (M063): ' || to_char(m063_r_db) || '.');		--||' : '||to_char(m063_r_db-least(m003_r_db,m063_r_db))	
            elsif m063_r_db>0 and m003_r_db_for_m063 > 0 then
                dbms_output.put_line('ESA szektorkód változás (M063): ' || to_char(m063_r_db) || ' : ' || to_char(m063_r_db - m003_r_db_for_m063) || '.');		
            end if;	*/
            if MP65_r_db > 0 and m003_r_db_for_MP65 = 0 then
                dbms_output.put_line('ESA szektorkód változás (MP65): ' || to_char(MP65_r_db) || '.');		--||' : '||to_char(m063_r_db-least(m003_r_db,m063_r_db))	
            elsif MP65_r_db > 0 and m003_r_db_for_MP65 > 0 then
                dbms_output.put_line('ESA szektorkód változás (MP65): ' || to_char(MP65_r_db) || ' : ' || to_char(MP65_r_db - m003_r_db_for_MP65) || '.');		
            end if;	
            if ueleszt_db>0 then
                dbms_output.put_line('Újraélesztések (UELESZT): '||to_char(ueleszt_db) || '.');
            end if;
            if ifrs_db>0 then
                dbms_output.put_line('Új IFRS nyilatkozatok (MVB39): '||to_char(ifrs_db) || '.');
            end if;	
            if cegv_db>0 and m003_r_db_for_cegv = 0 then
                dbms_output.put_line('Cégjegyzékszám változások (CEGV): '||to_char(cegv_db) || '.');
            elsif cegv_db>0 and m003_r_db_for_cegv > 0 then
                dbms_output.put_line('Cégjegyzékszám változások (CEGV): ' || to_char(cegv_db) || ' : ' || to_char(cegv_db - m003_r_db_for_cegv) || '.');
            end if;
            eddig:=52;      
        else
            dbms_output.put_line('A mai nap folyamán nem került Q01-es cég leválogatásra.');
        end if;
        --insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ: ','6. Elsõ melléklet kész'||to_char(sysdate,'YYYYMMDD HH24:MI:SS'));
--Ha nincsen Q01 sor (mert pl kiesett mind a ciklus belsejében!)
--Ha az elején már betettem a nemleges sort, akkor 1 db lesz, azaz itt nem kerül be még egyszer.
        if levalogatva_1=0 then
		     eddig:=53;
		      sor:='Q01,'||to_char(sysdate,'YYYYMMDD')||',15302724,'||to_char(sysdate,'YYYYMMDD')||',N';
			  eddig:=54;
              $if $$debug_on $then       
                  insert into vb_rep.mnb_napi_debug
                    values ('Q01',q01mellekletnev,1,sor);
              $else              
                  insert into vb_rep.mnb_napi
                    values ('Q01',q01mellekletnev,1,sor);
              $end  
			  levalogatva_1:=1;
			  eddig:=55;
		end if;	  
        
		
        for i in (select 
                        rownum rn, m003_je,  m003_ju, mv07, mv501_je, mv501_ju, dtol, jeju_r, kulf_ju, orszagkod,lezarva
                           from (select m003_je,to_char(m003_ju) m003_ju,mv07,mv501_je,mv501_ju,dtol,jeju_r,'0' kulf_ju, null orszagkod,decode(dig,null,'0','1') lezarva from
                                 vb.f003_juje,vb.f003 where f003.m003=f003_juje.m003_je
                                             and  (jeju_r  >= utolso_futas or datum_r>utolso_futas)
                                             and  substr(m0491,1,2)!='23' and m0491!='961' and m0491!='811'
--                        and
--                           (m040 not in ('0','9') or to_char(m040k,'YYYY')=to_char(sysdate,'YYYY'))
                union
                        select m003_je,to_char(m003_ju),mv07,mv501_je,mv501_ju,dtol,jeju_r,'0' kulf_ju, null orszagkod,decode(dig,null,'0','1')
                        from f003_juje, (select m003,param_dtol from vb_rep.vb_app_init where m003 is not null) c
                        where c.m003=f003_juje.m003_je and to_date(param_dtol,'YYYY-MM-DD hh24:mi:ss')>=utolso_futas  -- élõ, vagy az adott naptári évben szûnt meg
                union   --a jogutód nélkül megszûnt, de külföldi jogutódos szervezeteket is hozzá teszi a jeju rekordokhoz
                        select m003_je,'00000001' m003_ju,null mv07,					   
						decode(j.atalak,'A','220','B','230','E','235','K','830','L','835','O','240','S','285','V','280','990') mv501_je,
					    decode(j.atalak,'A','120','B','930','E','190','K','130','L','135','O','140','S','185','V','180','990') mv501_ju,
					    g.m040k dtol,j.datum_r,kulf_ju, null orszagkod,'0' from vb_ceg.jogutod j, vb.f003 g
                        where kulf_ju='1' and g.m003=m003_je and g.m040k_r>=utolso_futas and g.m040='9'
				union  -- a vb_app_init-be fölvett törzsszámok közül a külföldi jogutódos megszûnések
                       select  distinct m003_je,'00000001' m003_ju,null mv07,
						decode(j.atalak,'A','220','B','230','E','235','K','830','L','835','O','240','S','285','V','280','990') mv501_je,
					    decode(j.atalak,'A','120','B','930','E','190','K','130','L','135','O','140','S','185','V','180','990') mv501_ju,
					   r.m040k dtol,
                       j.datum_r,kulf_ju,null orszagkod,'0' from vb_ceg.jogutod j, vb_rep.vb_app_init g, vb.f003 r
                        where kulf_ju='1' and g.m003=m003_je 
                        and g.m003=r.m003 and datum_r=(select max(datum_r) from vb_ceg.jogutod where m003_je=j.m003_je)
                        and to_date(param_dtol,'YYYY-MM-DD hh24:mi:ss')>=utolso_futas))				
 
        loop
			sor:='Q02,'||
			   to_char(sysdate,'YYYYMMDD')||',15302724,'||to_char(sysdate,'YYYYMMDD')||',E,KSHJEJU,@KSHJEJU'||
			   substr(to_char(sorok1+i.rn,'0999999'),2,7)||','||
			   to_char(i.m003_je)||','||
			   i.m003_ju||','||
			   i.mv07||','||
			   i.mv501_je||','||
			   i.mv501_ju||','||
			   to_char(i.dtol,'YYYYMMDD')||','||
			   to_char(i.jeju_r,'YYYYMMDD')||','||
			   i.kulf_ju||','||
			   i.orszagkod||','||
			   i.lezarva;
			   eddig:=56;
               
	        if i.kulf_ju='1' then
			    kulf_db:=kulf_db+1;
			end if;
			begin
				   insert into  $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end (kod,filename,sorszam,rekord)
					   values ('Q02',q02mellekletnev,sorok1+i.rn,sor);                    
			exception
				when others then
					serr:=sqlcode;
					errmsg:=substr(sqlerrm,1,100);
					insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ: '||programneve||datum_kar,errmsg);
					commit;
			end;  
        end loop;
		commit; 
		select count(*) into levalogatva_2 from  $if $$debug_on $then vb_rep.mnb_napi_debug  $else vb_rep.mnb_napi $end  where kod='Q02' and 
		substr(rekord,5,8)=to_char(sysdate,'YYYYMMDD');
		eddig:=57;
		if levalogatva_2=0 then          
	  --nincs második melléklet
			begin
				sor:='Q02,'||to_char(sysdate,'YYYYMMDD')||',15302724,'||to_char(sysdate,'YYYYMMDD')||',N';
				insert into $if $$debug_on $then vb_rep.mnb_napi_debug  $else vb_rep.mnb_napi $end
						values ('Q02',q02mellekletnev,1,sor);
				levalogatva_2:=0;--1 helyett nulla		
			exception
				when others then
					  serr:=sqlcode;
					  errmsg:=substr(sqlerrm,1,100);
					  insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ: '||programneve||datum_kar,errmsg);
			end;         
			commit;
		end if;  --q02db+sorok1
		eddig:=58;
        dbms_output.put_line('---');
        if(levalogatva_2 != 0) then
            dbms_output.put_line('Leválogatott Q02-es jogelõd-jogutód cégpárok: ' || to_char(levalogatva_2) || ', amelyek közül külföldi jogutód: ' || to_char(kulf_db) || '.');	
        else
            dbms_output.put_line('A mai nap folyamán nem került Q02-es cégpár leválogatásra.');
        end if;
        
		begin
			update vb_rep.vb_app_init 
				 set 
				 param_ertek=datum_kar  --az utolsó futás dátuma
			where 
				 alkalmazas='MNB napi változáslista küldése' 
			and  program='mnb_EBEAD.sql' 
			and  $if $$debug_on $then param_nev='utolso_futas - debug' $else param_nev='utolso_futas' $end
			and   sysdate>to_date(param_dtol,'YYYY-MM-DD HH24:MI:SS'); 
			--update vb_rep.vb_app_init set m003=null;
			commit;
		exception when others then
			serr:=sqlcode;
			insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ: '||programneve||datum_kar,'app_init_update a hiba');
		end;
		rekordsorszam:=rekordsorszam-kihagyott;
		eddig:=59;
		begin
		   $if $$debug_on $then
			vb.mod_szam_tolt('K865', tablaneve, rekordsorszam, 'MNB napi változáslista küldése, debug mód  Kihagyott: ' || to_char(kihagyott), programneve || verzio, datum, 'V');
		   $else
		     vb.mod_szam_tolt('K865', tablaneve, rekordsorszam, 'MNB napi változáslista küldése  Kihagyott: ' || to_char(kihagyott), programneve || verzio, datum, 'V');
           $end		   
		exception when others then
			insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ: '||datum_kar,'mod_szam_tolt a hiba');
		end;
		eddig:=60;
		commit;
exception
    when others then
        serr:=sqlcode;
        insert into vb.vb_uzenet values (serr,'mnb napi adatküldõ: '||programneve||datum_kar,'A program egészét érintõ hiba: valami leválogatási hiba van a '||to_char(eddig)||' számnál a'||to_char(w_m003)||' törzsszámon');
        commit;
        dbms_output.put_line(to_char(serr)||'  valami leválogatási hiba történt '||to_char(eddig)||' számnál a'||to_char(w_m003)||' törzsszámon');
        $if $$debug_on $then
		   vb.mod_szam_tolt('K865', tablaneve, rekordsorszam, 'DEBUG Módban HIBÁVAL ÉRT VÉGET ' || to_char(eddig) || ' számnál a ' || to_char(w_m003) || ' törzsszámon:' || to_char(serr), programneve || verzio, datum, 'V');
        $else
		   vb.mod_szam_tolt('K865', tablaneve, rekordsorszam, 'HIBÁVAL ÉRT VÉGET ' || to_char(eddig) || ' számnál a ' || to_char(w_m003) || ' törzsszámon:' || to_char(serr), programneve || verzio, datum, 'V');
		$end
		commit;
end;
/                                                       