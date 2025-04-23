set termout on
set linesize 1000
set trimspool on
set heading off
set echo off
set serveroutput on size 1000000

alter session set plsql_ccflags='debug_on:false';--true

DECLARE
    verzio varchar2(25):=' 70. verzi�';
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
    napok_szama_visszamenoleg number:=1;  -- alap�rtelmezett a tegnapi nap �ta
    serr             number:=0;               --hibak�d
    errmsg  varchar2(100);                 -- hiba�zenet
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
    kulondb number:=0;      --a k�l�n list�n k�rt t�rzssz�mok sz�ma
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
    nev_hossz number:=250;  --2016.05.23.-t�l, m�r az 50-es verzi�ban is.
	rnev_hossz number:=250;  --az okozza az elt�r�st, hogy a r�vid nevet eddig nem v�gtam le!
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
								  nvl(letszam_R, regi_datum),--Szil�gyi �d�m 2022.10.19. _R _h helyett
								  nvl(arbev_r, regi_datum),
								  nvl(m0783_r, regi_datum),
								  nvl(m0583_r, regi_datum),
								  nvl(MP65_r, regi_datum), --m063_r --Szil�gyi �d�m 2023.09.27.
								  nvl(ueleszt_R, regi_datum),--Szil�gyi �d�m 2022.10.18. _R
								  nvl(cegv_r, regi_datum),
								  nvl(mvb39_r, regi_datum)
								  )>= utso_futas
								  or (m0491_r>= utso_futas and m0491_f!='06')
                         )						
                     and
                        substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811' 
                    --    and M0491 = '113' and M040 not in ('0', '9') --csak tesztel�shez
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
                        select m003 from vb_rep.vb_app_init where	--ezeket a t�rzssz�mokat kihagyja az adatk�ld�sb�l (egyszer)				
							   program = 'mnb_EBEAD.sql'--programneve_1
							   and to_date(param_dtol,'YYYY-MM-DD HH24:MI:SS') > utso_futas
							       and param_nev = '-m003' 
							   );

						
	TYPE gszr_table IS TABLE OF gszr_cur %ROWTYPE
    INDEX BY PLS_INTEGER;
	gszr_rec gszr_cur%rowtype;				   
    Type q01_rec_type is record (Q01 varchar2(3 char),                        -- adatgy�jt�s k�dja
	                             datum1 varchar2(8 char),                    -- vonatkoz�si id� 8 hosszon
                                 KSH_torzsszam varchar2(8 char),             -- KSH t�rzssz�ma
                                 kitoltes_datum varchar2(8 char),            -- kit�lt�s d�tuma 8 hosszon
                                 kshtorzs varchar2(20 char),                 -- 1 karakter fixen: E, t�blak�d: @KSHTORZS
                                 rekordsorszam varchar2(7 char),             -- sorsz�m 7 karakteren el�null�zva  a kurzor rekordsz�m�b�l levonva az eddig �tl�pett rekordok sz�m�t
                                 torzsszam varchar2(8 char),                 -- t�rzssz�m
                                 gfo varchar2(3 char),                       -- gazd.forma
								 gfo_hataly varchar2(8 char),                -- gfo hat�ly d�tuma
								 megyekod varchar2(2 char),                  --sz�khely megyek�dja, 
                                 megyekod_hataly varchar2(8 char),           --a megyek�d hat�ly d�tuma
                                 nev   varchar2(250 char),                   --n�v
                                 nev_h varchar2(8 char),                     --n�v hat�lya
                                 rnev  varchar2(250 char),                    --r�vid n�v
                                 RNEV_H varchar2(8 char),                    --r�vid n�v hat�lya
                                 M054_SZH varchar2(4 char),                  --sz�khely ir�ny�t� sz�m
                                 telnev_szh varchar2(30 char),--20 volt Szil�gyi �d�m
                                 utca_szh varchar2(100 char),--80 volt Szil�gyi �d�m
                                 SZEKHELY_H varchar2(8 char),                --sz�khely c�m hat�lya            
                                 M054_LEV  varchar2(4 char),                 --levelez�si c�m ir�ny�t� sz�ma
                                 telnev_lev   varchar2(30 char),--20 volt Szil�gyi �d�m
                                 utca_lev varchar2(80 char),                             
                                 LEVELEZESI_R varchar2(8 char),              --levelez�si c�m hat�lya (rendszerbe ker�l�se)
                                 m054_pf_lev varchar2(4 char),               -- postafi�kos levelez�si c�m ir�ny�t� sz�ma 
                                 telnev_pf_lev varchar2(30 char),  --20 volt Szil�gyi �d�m          -- postafi�kos levelez�si c�m telep�l�s neve
                                 pfiok_lev varchar2(10 char),                 -- postafi�k
                                 leV_PF_R varchar2(8 char),                  -- pf. c�m hat�lya (rendszerbe ker�l�se)
                                 M040 varchar2(1 char),                      -- m�k�d�s �llapotk�dja 
								 m040k varchar2(8 char),                     -- �llapotk�d hat�lya
                                 mukodv varchar2(8 char),                    -- m�k�d�s v�ge
                                 m025 varchar2(2 char),                      -- l�tsz�m kateg�ria
                                 letszam_h varchar2(8 char),                 --l�tsz�m kateg�ria besorol�s d�tuma
                                 m026 varchar2(1 char),                      --�rbev�tel kateg�ria
                                 arbev_h varchar2(8 char),                   --�rbev�tel kateg�ria hat�lya
                                 m009_szh varchar2(5 char),                  --sz�khely telep�l�s k�d+cdv 5 jegyen
                                 alakdat varchar2(8 char),                    --alakul�s d�tuma
                                 m0781 varchar2(4 char),                     --admin szak�g 2008
                                 m0781_h varchar2(8 char),                   --2008-as besorol�si k�d hat�lya
                                 m058_j varchar2(4 char),                    --janus TE�OR
                                 m0581_j_h varchar2(8 char),                 --janus TE�OR hat�lya (azonos a stat TE�OR hat�ly�val)
                                 MP65 varchar2(5 char),   --m063 varchar2(2) --ESA szektork�d
                                 MP65_H varchar2(8 char),       --m063_h     --ESA szektork�d hat�lya
                                 ueleszt   varchar2(8 char),                 --�jra�leszt�s hat�lya
                                 m003_r varchar2(8 char),                    --rendszerbe ker�l�s d�tuma
                                 datum2 varchar2(8 char),                     --napi lev�logat�s d�tuma
                                 statteaor varchar2(4 char),                 --statisztikai TE�OR
                                 stathataly varchar2(8 char),                --statisztikai TE�OR hat�lya
								 cegjegyz   varchar2(20 char),                --c�gjegyz�k sz�m
								 cegjegyz_h  varchar2(8 char),               --c�gjegyz�ksz�m hat�lya
								 mvb39       varchar2(1 char),               --IFRS nyilatkozat
								 mvb39_h     varchar2(8 char),                --IFRS nyilatkozat hat�lya
                                 ORSZ varchar2(2 char),                         --Orsz�gk�d
                                 LETSZAM varchar2(6 char),                      --L�tsz�m
                                 ARBEV varchar2(8 char),                        --�rbev�tel
                                 M0783 varchar2(4 char),                     --Adminisztrat�v TE�OR 2025
                                 M0783_H varchar2(8 char),                   --2025-�s besorol�si k�d hat�lya
                                 M0583 varchar2(4 char),                     --Statisztikai TE�OR 2025
                                 M0583_H varchar2(8 char),                   --2025-�s besorol�si k�d hat�lya
                                 stat_szakag_t�pus varchar2(2 char),        --0 = Statisztikai TE�OR, 1 = nemzeti sz�ml�s TE�OR
                                 EMAIL varchar2(90 char),                     --E-mail c�m
                                 EMAIL_H varchar2(8 char),                   --E-mail c�m hat�ly kezdete
                                 Cegkapu varchar2(90 char),                     --C�gkapu
                                 Cegkapu_H varchar2(8 char),                   --C�gkapu hat�ly kezdete
                                --rendszerbe ker�l�sek a feldolgoz�shoz
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
                                 UELESZT_R date--Szil�gyi �d�m 2022.10.19.
	                            );
    mnb_rec q01_rec_type;
	nsz_db number;                                                      -- h�ny nemzeti sz�ml�s te�or-rekordja van a szervezetnek?
	sqlmsg varchar2(50);
	eddig number:=0;
	kulf_db number:=0; --hat�ron �tny�l� megsz�n�sek hat�ron t�li jogut�djainak darabsz�ma

BEGIN
	programneve:= $if $$debug_on $then 'mnb_EBEAD_debug.sql' $else 'mnb_EBEAD.sql' $end;
	$if $$debug_on $then dbms_output.put_line('DEBUG m�d bekapcsolva'); $end
	
    select to_char(sysdate,'YYYY-MM-DD HH24:MI:SS'),sysdate into datum_kar,datum from dual;
 
    eddig:=1;      
	begin
	  $if $$debug_on $then
	    vb.mod_szam_tolt('K865', 'vb_rep.mnb_napi', sorok, 'MNB napi v�ltoz�slista k�ld�se,debug m�d ', programneve || verzio,datum, 'K');
	  $else
	    vb.mod_szam_tolt('K865', 'vb_rep.mnb_napi', sorok, 'MNB napi v�ltoz�slista k�ld�se', programneve || verzio,datum, 'K');
	  $end
        commit;
    exception when others then
        serr:=sqlcode;
        insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||datum_kar,'mod_szam_tolt a hiba');
        commit; 
    end;
    
	eddig:=2;
    begin        
      --  $if $$debug_on $then
       --     select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_kuldes from vb_rep.vb_app_init
        --            where alkalmazas='MNB napi v�ltoz�slista k�ld�se' 
         --            and   program='mnb_kuld.sql'
          --           and param_nev='utolso_futas - debug';			
       -- $else
            select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_kuldes from vb_rep.vb_app_init
                    where alkalmazas='MNB napi v�ltoz�slista k�ld�se' 
                     and   program='mnb_kuld.sql'
                     and param_nev='utolso_futas';
       -- $end                                            
    exception when others then 
	    dbms_output.put_line(sqlerrm);
		dbms_output.put_line('A vb_app_init t�bl�b�l az utols� k�ld�s d�tum�t nem lehetett olvasni');
        utolso_kuldes:=sysdate-1;
    end; 
    
	eddig:=3;
 --  Mikor futott utolj�ra a lek�rdez�s?
    begin        
        $if  $$debug_on $then
            --dbms_output.put_line('utolso_futasba-debug');
            select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_futas from vb_rep.vb_app_init
                    where alkalmazas='MNB napi v�ltoz�slista k�ld�se' 
                     and   program='mnb_EBEAD.sql'
                     and param_nev='utolso_futas - debug';
        $else
            --dbms_output.put_line('utolso_futasba-�les');
            select to_date(param_ertek,'YYYY-MM-DD HH24:MI:SS') into utolso_futas from vb_rep.vb_app_init
                    where alkalmazas='MNB napi v�ltoz�slista k�ld�se'
                     and   program='mnb_EBEAD.sql'
                     and param_nev='utolso_futas';
        $end                                            
    exception when others then 
        utolso_futas:=sysdate-1;
    end;    

    dbms_output.put_line('Utols� lev�logat�s: ' || to_char(utolso_futas, 'YYYY-MM-DD HH24:MI:SS') || '.');	
    dbms_output.put_line('Utols� felt�lt�s: ' || to_char(utolso_kuldes, 'YYYY-MM-DD HH24:MI:SS') || '.');	
	eddig:=4;
      
--t�r�lni az utols� sikeres felt�lt�sn�l r�gebbi rekordokat
     begin
            delete from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where 
			substr(rekord,5,8)<=to_char($if $$debug_on $then utolso_futas $else utolso_kuldes $end,'YYYYMMDD');
            commit;
     exception when others then  
            serr:=sqlcode;           
            insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld� '||programneve||datum_kar, 'r�gi elk�ld�tt rekord t�rl�s hiba'||to_char(utolso_kuldes,'YYYYMMDD'));
            commit; 
     end; 
     eddig:=5;
--   rekordt�pus nev�nek �sszerak�sa(a mell�kletnevek �sszerak�sa )      
    q01mellekletnev:='Q01'||substr(to_char(datum,'YYYYMMDD'),4,5)||'15302724';
    q02mellekletnev:='Q02'||substr(to_char(datum,'YYYYMMDD'),4,5)||'15302724';
       
--t�r�lni az esetleges �res k�ldend�ket, hogy ne legyen 2 els� sor!
    begin
        delete from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where q01mellekletnev=filename and substr(rekord,32,1)='N';
        delete from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where q02mellekletnev=filename and substr(rekord,32,1)='N';
        commit;
    exception when others then    
        serr:=sqlcode;
        insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld� '||programneve||datum_kar,'puffersor t�rl�s hiba');
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
                        dbms_output.put_line('A Q01-es adat�tad�ssal kik�ldend� plusz c�gek sz�ma: ' || PLUSZ_Q01 || '.');
                     end if;
                     
                exception
                    when no_data_found then
                        dbms_output.put_line('Kiv�tel t�rt�nt a plusz c�gek sz�m�nak meghat�roz�sa sor�n.');
                end;
                eddig:=120;


   begin    
                select count(*) into MINUSZ_Q01
                    from vb_rep.vb_app_init where 
							   program = 'mnb_EBEAD.sql'--programneve_1
							   and to_date(param_dtol, 'YYYY-MM-DD HH24:MI:SS') > utolso_futas
							       and param_nev = '-m003';
                     if (MINUSZ_Q01 > 0) then
                        dbms_output.put_line('A Q01-es adat�tad�sb�l kivonand� c�gek sz�ma: ' || MINUSZ_Q01 || '.');
                     end if;
                     
                exception
                    when no_data_found then
                        dbms_output.put_line('Kiv�tel t�rt�nt a kivonand� c�gek sz�m�nak meghat�roz�sa sor�n.');
                end;
                eddig := 121;
                
                
    begin
                    select count(*) into ALAKDAT_MODSZAM
                    from vb.f003_hist3pr where datum > utolso_futas and alakdat != alakdat_u and M003 in (select M003 from VB.F003 where substr(M0491, 1, 2) != '23' and M0491 != '961' and M0491 != '811');
                     
                exception
                    when no_data_found then
                        dbms_output.put_line('Kiv�tel t�rt�nt az alakul�s d�tum�nak v�ltoz�s�nak sz�m�nak meghat�roz�sa sor�n.');
                end;
                eddig:=130;
 

 --k�ld�s v�ge azokra a lez�rt k�ldend� m0582-kre, amelyekre az el�z� fut�s �ta megv�ltozott az m0581, �s az nem egyenl� az m0582-vel
    for i in (--select t.rowid rn,t.m003 from vb.f003 g, $if $$debug_on $then vb.f003_m0582_debug $else vb.f003_m0582 $end t where 
	           select t.rowid rn,t.m003 from vb.f003 g,  vb.f003_m0582 t where
                  t.m003=g.m003 and m0582_hv is not null and t.kuldes_vege is null
                  and g.m0583_r>=utolso_futas --stat TE�OR m�dosult
                  union
                  --select t.rowid rn,t.m003 from vb.f003 g,$if $$debug_on $then vb.f003_m0582_debug $else vb.f003_m0582 $end t where 
				  select t.rowid rn,t.m003 from vb.f003 g, vb.f003_m0582 t where 
                  t.m003=g.m003 and t.m0582_r>=utolso_futas and t.kuldes_vege is null)   --NSZ TE�OR (vagy hat�lya) m�dosult
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
                INSERT INTO VB.VB_UZENET VALUES(serr,programneve||datum_kar,'Valami g�z van a lez�r�ssal:'||TO_CHAR(I.M003));
                commit;
                rollback;               
        end; 
		eddig:=161;
        sorok:=sorok+1;               
        if mod(sorok,100)=0 then
            begin
			$if $$debug_on $then
                vb.mod_szam_tolt('K865', 'vb_rep.mnb_napi', sorok, 'MNB napi v�ltoz�slista k�ld�se,debug m�d ', programneve || verzio, datum, 'M');
			$else
			    vb.mod_szam_tolt('K865', 'vb_rep.mnb_napi', sorok, 'MNB napi v�ltoz�slista k�ld�se', programneve || verzio, datum, 'M');
			$end
            exception when others then
                insert into vb.vb_uzenet values (serr,programneve||datum_kar,'mod_szam_tolt a hiba');
            end;
            commit;
        end if;                 
    end loop;
    if(sorok > 0) then
        dbms_output.put_line('Nemzeti sz�mla TE�OR k�d v�ltoz�sok sz�ma: ' || to_char(sorok) || '.');
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
        dbms_output.put_line('A t�bl�ban tal�lhat� - m�r lev�logatott - Q01-es �s Q02-es sorok sz�ma:');
        dbms_output.put_line('M�r t�bl�ban van kor�bbr�l Q01-es: ' || to_char(sorok));
        dbms_output.put_line('M�r t�bl�ban van kor�bbr�l Q02-es: ' || to_char(sorok1));
    end if;
    
	open gszr_cur(utolso_futas,programneve);
	--dbms_output.put_line('kurzor nyitva');
	fetch gszr_cur into gszr_rec;
	--dbms_output.put_line('fetch k�sz');
    if gszr_cur%notfound then
        null;
    else 
	--ha szerverhiba miatt nem futott a program, a visszamen� napokra �res �zeneteket kell k�pezni
	
	if sysdate-utolso_futas >2 then
	    dbms_output.put_line('Az utols� lev�logat�s �ta eltelt napok sz�ma: ' || to_char(sysdate-utolso_futas) || '.');
		for nap in 1..floor(sysdate-utolso_futas)
		loop
		    puffersorok:=puffersorok+1;
		end loop;
		commit;
	end if;
	--Innen pedig j�n az aktu�lis v�ltoz�sok lev�logat�sa
        --levalogatva:=sorok;
        rekordsorszam:=1;
		--dbms_output.put_line(to_char(rekordsorszam));
		eddig:=10;
        loop
            exit when gszr_cur%notfound;
			w_m003:=gszr_rec.m003;
            mnb_rec.Q01:='Q01';                                      -- adatgy�jt�s k�dja
            mnb_rec.datum1:=to_char(datum,'YYYYMMDD');               -- vonatkoz�si id� 8 hosszon
            mnb_rec.KSH_torzsszam:='15302724';                       -- KSH t�rzssz�ma
            mnb_rec.kitoltes_datum:=to_char(datum,'YYYYMMDD');       -- kit�lt�s d�tuma 8 hosszon
            mnb_rec.kshtorzs:='E,KSHTORZS,@KSHTORZS';                -- 1 karakter fixen: E, t�blak�d: @KSHTORZS
            mnb_rec.rekordsorszam:=substr(to_char(rekordsorszam,'0999999'),2,7);             -- sorsz�m 7 karakteren el�null�zva  a kurzor rtekordsz�m�b�l levonva az eddig �tl�pett rekordok sz�m�t
 		   
			w_statteaor:=null;
			w_stathataly:=null;
			w_hatalyvege:=null;
			w_stat_r:=null;
			w_kuldes_vege:=null;
			--dbms_output.put_line(to_char(gszr_rec.m003));
                begin
                    select 
					    to_char(gszr_rec.m003),         -- t�rzssz�m, 
                        m0491,
                        to_char(m0491_h, 'YYYYMMDD'),    --GFO hat�ly
                        m005_szh,                       --sz�khely megyek�dja, 
                        to_char(m005_szh_h, 'YYYYMMDD'), --a megyek�d hat�ly d�tuma
                        case when instr(nev, '''')>0 or instr(nev, '"')>0 or instr(nev, ',')>0 then
                          '"'||replace(substr(nev, 1, nev_hossz - (REGEXP_COUNT(nev, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(nev, 1, nev_hossz)
		                end,                           -- n�v
   					    to_char(nev_h, 'YYYYMMDD'),     --n�v hat�lya
                        case when instr(rnev, '''')>0 or instr(rnev,'"')>0 or instr(rnev,',')>0 then
                          '"'||replace(substr(rnev, 1, rnev_hossz - (REGEXP_COUNT(rnev, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(rnev, 1, rnev_hossz)
		                end,                         --r�vid n�v
                        to_char(rnev_h, 'YYYYMMDD'),  --r�vid n�v hat�lya
                        rtrim(to_char(m054_szh)),    --sz�khely ir�ny�t� sz�m
                        case when instr(telnev_szh, '''')>0 or instr(telnev_szh,'"')>0 or instr(telnev_szh,',')>0 then
                          '"'||replace(substr(telnev_szh, 1, telnev_hossz - (REGEXP_COUNT(telnev_szh, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(telnev_szh,1,telnev_hossz)
		                end,                                   --sz�khely
                        case when instr(utca_szh,'''') > 0 or instr(utca_szh,'"')>0 or instr(utca_szh,',')>0 then
                          '"'||replace(substr(utca_szh, 1, utca_hossz - (REGEXP_COUNT(utca_szh, '"') + 2)),'"', '""')||'"'
		                else
		                   substr(utca_szh,1,utca_hossz)
		                end,                                    --sz�khely utca, h�zsz�m
                        to_char(szekhely_h,'YYYYMMDD'),         --sz�khely c�m hat�lya   		
                        rtrim(to_char(M054_LEV)),               --levelez�si c�m ir�ny�t� sz�ma
                        case when instr(telnev_lev, '''') > 0 or instr(telnev_lev, '"') > 0 or instr(telnev_lev, ',') > 0 then
                          '"'||replace(substr(telnev_lev, 1, telnev_hossz - (REGEXP_COUNT(telnev_lev, '"') * 2 + 2)),'"','""')||'"'
		                else
		                   substr(telnev_lev, 1, telnev_hossz)
		                end,                                     --levelez�si c�m telep�l�s n�v
                        case when instr(utca_lev, '''') > 0 or instr(utca_lev, '"') > 0 or instr(utca_lev, ',') > 0 then
                          '"'||replace(substr(utca_lev, 1, utca_hossz - (REGEXP_COUNT(utca_lev, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(utca_lev, 1, utca_hossz)
		                end,                                      --levelez�si c�m utca
                        decode(levelezesi_h, null, to_char(levelezesi_r, 'YYYYMMDD'), to_char(levelezesi_h, 'YYYYMMDD')),  --levelez�si c�m hat�lya (rendszerbe ker�l�se)
                        to_char(m054_pf_lev),                     --postafi�kos levelez�si c�m ir�ny�t� sz�ma 
                        case when instr(telnev_pf_lev, '''') > 0 or instr(telnev_pf_lev, '"') > 0 or instr(telnev_pf_lev, ',') > 0 then
                          '"'||replace(substr(telnev_pf_lev, 1, telnev_hossz - (REGEXP_COUNT(telnev_pf_lev, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(telnev_pf_lev,1,telnev_hossz)
		                end,                                        -- postafi�kos levelez�si c�m telep�l�s neve
                        case when instr(pfiok_lev, '''') > 0 or instr(pfiok_lev, '"') > 0 or instr(pfiok_lev, ',') > 0 then
                          '"'||replace(substr(pfiok_lev, 1, pfiok_lev_hossz - (REGEXP_COUNT(pfiok_lev, '"') + 2)), '"', '""')||'"'
		                else
		                   substr(pfiok_lev, 1, pfiok_lev_hossz)
		                end,                                           -- postafi�k			
                        decode(lev_pf_r, null, '', to_char(lev_pf_r, 'YYYYMMDD')), --pf. c�m hat�lya (rendszerbe ker�l�se)
                        m040,                                                  --m�k�d�s �llapotk�dja 
						to_char(m040k, 'YYYYMMDD'),                                                         --m�k�d�si k�d hat�lya
                        decode(mukodv, null, '', to_char(mukodv,'YYYYMMDD')),             --m�k�d�s v�ge
                        m025,                                   -- l�tsz�m kateg�ria
                        decode(letszam_h, null, '', to_char(letszam_h, 'YYYYMMDD')), --l�tsz�m kateg�ria besorol�s d�tuma			
                        m026,            --�rbev�tel kateg�ria	
						decode(arbev_h, null, to_char(alakdat, 'YYYYMMDD'), to_char(arbev_h, 'YYYYMMDD')),   --�rbev�tel kateg�ria hat�lya 
						m009_szh,
						decode(alakdat, null, '', to_char(alakdat, 'YYYYMMDD')),            --alakul�s d�tuma
                        m0781,                                                          --admin szak�g 2008
                        to_char(m0781_h, 'YYYYMMDD'),   --2008-as besorol�si k�d hat�lya
                        m058_j,                        --janus TE�OR
                        to_char(m0581_h, 'YYYYMMDD'),   --janus TE�OR hat�lya (azonos a stat TE�OR hat�ly�val)
                        decode(MP65, 'S9900', null, MP65),   --decode(m063,null,'90',m063)                            --ESA szektork�d
                        to_char(MP65_H, 'YYYYMMDD'),  --to_char(decode(m063_h,null,alakdat,m063_h),'YYYYMMDD')  --ESA szektork�d hat�lya
                        decode(ueleszt, null, '', to_char(ueleszt,'YYYYMMDD')),  --�jra�leszt�s hat�lya
                        decode(m003_r, null, '', to_char(m003_r,'YYYYMMDD')),    --rendszerbe ker�l�s d�tuma
                        to_char(datum, 'YYYYMMDD'),     --lev�logat�s d�tuma
                        M0581_J,                         --statisztikai TE�OR 2025. janu�r 01-t�l janus
                        to_char(M0583_H, 'YYYYMMDD'),   --stathat�ly 2025
					    cegv,                           -- c�gjegyz�k sz�m
						to_char(cegv_h, 'YYYYMMDD'),     -- hat�lya
						nvl(mvb39, '0'),                 -- IFRS nyilatkozat ha null, akkor legyen 0
						nvl(to_char(mvb39_h, 'YYYYMMDD'),case when to_char(alakdat,'YYYY') < '2016' then '20160101' else to_char(alakdat,'YYYYMMDD') end),    -- hat�lya ha nincsen akkor az alakul�s d�tuma kiv�ve, ha az alakul�s 2016.01.01-n�l r�gebbi, akkor 2016.01.01
						null,                           --Orsz�gk�d lesz m�sik t�bl�b�l
                        nvl(to_char(LETSZAM), 'N/A'),
                        nvl(to_char(ARBEV), 'N/A'),
                        M0783, --'0111', --M0781, --M0783,                              --Adminisztrat�v TE�OR 2025
                        to_char(M0783_H, 'YYYYMMDD'), --'20250101',--to_char(M0781_H, 'YYYYMMDD'),
                        M0583, --'0111', --M0581, --M0583,                          --Statisztikai TE�OR 2025
                        to_char(M0583_H, 'YYYYMMDD'), --'20250101',--to_char(M0581_H, 'YYYYMMDD'),           
                        0,                           --0 = Statisztikai TE�OR, 1 = nemzeti sz�ml�s TE�OR
                        null,                          --E-mail c�m lesz m�sik t�bl�b�l
                        null,                           --E-mail c�m kezd� hat�lya lesz m�sik t�bl�b�l    
                        null,                           --C�gkapu lesz m�sik t�bl�b�l
                        null,                           --C�gkapu kezd� hat�lya lesz m�sik t�bl�b�l
                        -- rendszerbe ker�l�sek a vizsg�latokhoz:	
						m003_r,                         -- t�rzssz�m rendszerbe ker�l�se
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
                        letszam_R,--Szil�gyi �d�m 2022.10.19 _R _h helyett
						cegv_r,
						mvb39_r,
                        UELESZT_R--Szil�gyi �d�m 2022.10.19.
                    into 
					    mnb_rec.torzsszam,            -- 8
                        mnb_rec.gfo,                  -- 3
                        mnb_rec.gfo_hataly,           --GFO hat�ly  8
                        mnb_rec.megyekod,             --sz�khely megyek�dja,  2
                        mnb_rec.megyekod_hataly,      --a megyek�d hat�ly d�tuma  8
                        mnb_rec.nev,                  --n�v  250
                        mnb_rec.nev_h,                --n�v hat�lya  8
                        mnb_rec.rnev,                 --r�vid n�v  40
                        mnb_rec.RNEV_H,               --r�vid n�v hat�lya  8
                        mnb_rec.M054_SZH,             --sz�khely ir�ny�t� sz�m  4
                        mnb_rec.telnev_szh,           --sz�khely	20
                        mnb_rec.utca_szh,             --sz�khely utca, h�zsz�m  80
                        mnb_rec.SZEKHELY_H,           --sz�khely c�m hat�lya 	8
                        mnb_rec.M054_LEV,	          --levelez�si c�m ir�ny�t� sz�ma  4
                        mnb_rec.telnev_lev,           --levelez�si c�m telep�l�s n�v	20
                        mnb_rec.utca_lev,             --levelez�si c�m utca	   80
                        mnb_rec.LEVELEZESI_R,         --levelez�si c�m rendszerbe ker�l�se	8
                        mnb_rec.m054_pf_lev,          --postafi�kos levelez�si c�m ir�ny�t� sz�ma   8
                        mnb_rec.telnev_pf_lev,        --postafi�kos levelez�si c�m telep�l�s neve   4
                        mnb_rec.pfiok_lev,            --postafi�k    20
                        mnb_rec.leV_PF_R,             --pf. c�m hat�lya (rendszerbe ker�l�se)    8
                        mnb_rec.M040,                 --m�k�d�s �llapotk�dja      
                        mnb_rec.m040k,                --m�k�d�si �llapot hat�lya	8					
                        mnb_rec.mukodv,               --m�k�d�s v�ge                8
                        mnb_rec.m025,                 --l�tsz�m kateg�ria           2
                        mnb_rec.letszam_h,            --l�tsz�m kateg�ria besorol�s d�tuma  8
                        mnb_rec.m026,                 --�rbev�tel kateg�ria    1
						mnb_rec.arbev_h,              --�rbev�tel kateg�ria hat�lya    8
						mnb_rec.m009_szh,             --sz�khely telep�l�s k�dja        5
                        mnb_rec.alakdat,              --alakul�s d�tuma                8
                        mnb_rec.m0781,                --admin szak�g 2008                 4
                        mnb_rec.m0781_h,              --2008-as besorol�si k�d hat�lya    8
                        mnb_rec.m058_j,               --janus TE�OR                        4
                        mnb_rec.m0581_j_h,            --janus TE�OR hat�lya (azonos a stat TE�OR hat�ly�val)   8
                        mnb_rec.MP65,        --m063        --ESA szektork�d                       2
                        mnb_rec.MP65_H,      --m063_h         --ESA szektork�d hat�lya                8
                        mnb_rec.ueleszt,              --�jra�leszt�s hat�lya                  8
                        mnb_rec.m003_r,               --szervezet rendszerbe ker�l�se         8
                        mnb_rec.datum2,               --napi lev�logat�s d�tuma                8
                        mnb_rec.statteaor,            --statisztikai TE�OR                     4
                        mnb_rec.stathataly,           --statisztikai TE�OR hat�lya             8
						mnb_rec.cegjegyz,             --c�gjegyz�k sz�m
						mnb_rec.cegjegyz_h,           --c�gjegyz�ksz�m hat�lya
						mnb_rec.mvb39,                --IFRS nyilatkozat
						mnb_rec.mvb39_h,          	  --IFRS hat�lya
                        mnb_rec.ORSZ,                 --Orsz�gk�d
                        mnb_rec.LETSZAM,              --L�tsz�m
                        mnb_rec.ARBEV,                --�rbev�tel
                        mnb_rec.M0783,                 --Adminisztrat�v TE�OR 2025
                        mnb_rec.M0783_H,               --2025-�s besorol�si k�d hat�lya
                        mnb_rec.M0583,                 --Statisztikai TE�OR 2025
                        mnb_rec.M0583_H,               --2025-�s besorol�si k�d hat�lya
                        mnb_rec.stat_szakag_t�pus,     --0 = Statisztikai TE�OR, 1 = nemzeti sz�ml�s TE�OR
                        mnb_rec.EMAIL,                 --E-mail c�m
                        mnb_rec.EMAIL_H,               --E-mail c�m hat�ly kezdete
                        mnb_rec.Cegkapu,               --C�gkapu
                        mnb_rec.Cegkapu_H,             --C�gkapu hat�ly kezdete
						-- rendszerbe ker�l�sek a vizsg�latokhoz d�tum t�pus�ak:
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
                        mnb_rec.UELESZT_R--Szil�gyi �d�m 2022.10.19.
                    from 
                        vb.f003 
                    where m003=gszr_rec.m003;
                exception
                    when others then 
                        serr:=sqlcode;
                        insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||to_char(gszr_rec.m003)||':'||datum_kar,' F003 select hiba');
                        commit;
						dbms_output.put_line('HIBA a ' || to_char(gszr_rec.m003) || ' t�rzssz�mn�l, amelynek �zenete: ' || sqlerrm || '.');
                        exit;
     			end;
				--telep�l�s k�d + cdv
                begin
                    select g.m009_szh||f.m009cdv into mnb_rec.m009_szh from f009_akt f,vb.f003 g where g.m003=gszr_rec.m003 and f.m009=g.m009_szh;
                exception
                    when no_data_found then                   --azon elk�pzelhetetl�l (?) ritka esetekben, ha az avar kori telep�l�st nem tal�ln�nk meg az F009-ben:
                        serr:=sqlcode;
                        insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||programneve||datum_kar||':'||to_char(mnb_rec.torzsszam)||' '||to_char(mnb_rec.m009_szh),' nincs cdv a telep�l�shez'||datum_kar);
                        commit;
                        mnb_rec.m009_szh:=mnb_rec.m009_szh||'X';
                end;                               --sz�khely telep�l�s k�d+cdv 5 jegyen	
                eddig:=11;				
                
                 begin
                    SELECT EELERHETOSEG, TO_CHAR(HATALY, 'YYYYMMDD') HATALY INTO mnb_rec.EMAIL, mnb_rec.EMAIL_H  from 
                    (select EELERHETOSEG, HATALY from VB.F003_EELERHETOSEG 
                    where M003 = gszr_rec.m003 and MVB42 in ('10', '11') 
                    order by M003, HATALY desc, MVB42, DATUM_R desc 
                    offset 0 row fetch first 1 rows only);
                    
                    if mnb_rec.EMAIL_H < mnb_rec.ALAKDAT then
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m e-mail c�m�nek hat�lya (' || mnb_rec.EMAIL_H || ') az alakul�s d�tum�ra v�ltozott (' || mnb_rec.ALAKDAT || '), mert att�l kor�bbi volt.');
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
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m� c�g c�gkapuja (' || mnb_rec.Cegkapu || ') megv�ltozott a t�rzssz�m#cegkapu form�tumra.');
                        mnb_rec.Cegkapu := mnb_rec.torzsszam || '#cegkapu';
                    end if;
                    
                    if mnb_rec.Cegkapu_H < mnb_rec.ALAKDAT then
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m c�gkapuj�nak hat�lya (' || mnb_rec.Cegkapu_H || ') az alakul�s d�tum�ra v�ltozott (' || mnb_rec.ALAKDAT || '), mert att�l kor�bbi volt.');
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
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja HU-r�l Z8 lett, mert a megyek�d (M005_SZH = ' || mnb_rec.megyekod || ') > 20.');
                     end if;
                     
                     if(mnb_rec.ORSZ = 'XX') then --M005_SZH
                        mnb_rec.ORSZ := 'Z8'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja XX-r�l Z8 lett, mert XX �rt�ket az MNB nem tud fogadni.');
                     end if;
                     
                     --Amennyiben �res orsz�gk�ddal van bent egy c�g a VB_CEG.VB_APEH_CIM adatb�zis t�bl�ban
                     if(mnb_rec.ORSZ is null and mnb_rec.megyekod < 21) then --M005_SZH
                        mnb_rec.ORSZ := 'HU'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja �resr�l HU lett, mert a megyek�d (M005_SZH = ' || mnb_rec.megyekod || ') < 21.');
                     end if;
                     
                     if(mnb_rec.ORSZ is null and mnb_rec.megyekod > 20) then --M005_SZH
                        mnb_rec.ORSZ := 'Z8'; 
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja �resr�l Z8 lett, mert a megyek�d (M005_SZH = ' || mnb_rec.megyekod || ') > 20.');
                     end if;
                     
                exception
                    when no_data_found then
                    
                         if(mnb_rec.ORSZ is null and mnb_rec.megyekod < 21) then --M005_SZH
                            mnb_rec.ORSZ := 'HU'; 
                            dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja �resr�l HU lett, mert a megyek�d (M005_SZH = ' || mnb_rec.megyekod || ') < 21.');
                         end if;
                     
                         if(mnb_rec.ORSZ is null and mnb_rec.megyekod > 20) then --M005_SZH
                            mnb_rec.ORSZ := 'Z8'; 
                            dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m orsz�gk�dja �resr�l Z8 lett, mert a megyek�d (M005_SZH = ' || mnb_rec.megyekod || ') > 20.');
                         end if;
                         
                end;
                eddig:=110;
                
                begin
                    if(mnb_rec.MP65 is null ) then 
                        mnb_rec.MP65_H := null; 
                        --dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m MP65_H �rt�ke �res lett.');
                     end if;
                end;
                
                begin
                    if (ALAKDAT_MODSZAM > 0) then
                        select M003 into M003_ALAKDAT_CHANGED from vb.f003_hist3pr where datum > utolso_futas and alakdat != alakdat_u and M003 = gszr_rec.m003;  
                        dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m alakul�s d�tuma megv�ltozott a k�vetkez�re: ' || to_char(mnb_rec.alakdat) || '.');
                    end if;     
                    
                    exception
                        when no_data_found then
                        null;
                             
                end;
                
-- A GSZR-rekord kigy�jt�s�nek v�ge.
-- Van-e v�ltoz�s a GSZR-rekordban vagy az F003_m0582-ben?		
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
                              values(serr,'mnb napi adatk�ld�:F003_m0582 lek�rdez�s'||datum_kar||':'||to_char(mnb_rec.torzsszam),errmsg);
                        commit; 
                end;
				eddig:=12;
                if nsz_db = 1 then
                 -- csak egy nsz-rekord van utols�
                    select m0582,m0582_h,m0582_hv,m0582_r, kuldes_vege into w_statteaor,w_stathataly,w_hatalyvege,w_stat_r, w_kuldes_vege from 
					vb.f003_m0582 g
                    where m003 = gszr_rec.m003 and m0582_r=(select max(m0582_r) from  vb.f003_m0582 
					                                      where m003 = gszr_rec.m003);
                elsif nsz_db > 1 then
                 --egy nap t�bb v�ltoz�s volt. Ezek k�z�l csak egy lehet nyitott: nem lez�rt, az kell
                    select m0582,m0582_h,m0582_hv,m0582_r, kuldes_vege  into w_statteaor,w_stathataly,w_hatalyvege,w_stat_r, w_kuldes_vege from 
					vb.f003_m0582 g
                    where m003 = gszr_rec.m003 and m0582_r=(select max(m0582_r) from vb.f003_m0582
					                                      where m003 = gszr_rec.m003)
                    and m0582_hv is null;
                end if;
			    eddig:=13;
--a hat�lyokat kalkul�lgatjuk:			  
                if nsz_db != 0 then
			if mnb_rec.statteaor = w_statteaor then
                    		dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m eset�n M0581_J = M0582 = ' || mnb_rec.statteaor || ', de a hat�ly M0583_H = ' || mnb_rec.stathataly || ' az NSZ TE�OR hat�lyra M0582_H = ' || to_char(w_stathataly, 'YYYYMMDD') || ' v�ltozott.');
                	else
                    		dbms_output.put_line('A ' || to_char(mnb_rec.torzsszam) || ' t�rzssz�m M0581_J = ' || mnb_rec.statteaor || ' �rt�ke �s M0583_H = ' || mnb_rec.stathataly || ' hat�lya az NSZ TE�OR k�dj�ra M0582 = ' || w_statteaor || ' �s annak hat�ly�ra M0582_H = ' || to_char(w_stathataly, 'YYYYMMDD') || ' v�ltozott.');
                	end if;
                        mnb_rec.statteaor := w_statteaor;
                        mnb_rec.stathataly := to_char(w_stathataly,'YYYYMMDD');
                    /*    mnb_rec.M0583 := w_statteaor;
                        mnb_rec.M0583_H := to_char(w_stathataly,'YYYYMMDD');
                        mnb_rec.stat_szakag_t�pus := '1';*/
						eddig:=17;
                end if; --nsz_db!=0
                 --l�tsz�m- �rbev�tel kateg�ria, �s ESA k�d
				--dbms_output.put_line('nsz-TE�OR k�sz');
                w_m025:='  ';
                w_m026:=' ';
              --  w_m063:='  ';
                eddig:=18;				
                --dbms_output.put_line('ESA k�d');            
                begin
                    select m025 into w_m025 from vb.f003_hist2 where m003=mnb_rec.torzsszam and datum=(select max(datum) from vb.f003_hist2 where m003=mnb_rec.torzsszam)
                    and rownum=1;
                exception
                    when no_data_found then
                       null;
                end; 
                eddig:=19;				
                --dbms_output.put_line('L�tsz�m kateg�ria');                  
                begin
                    select m026 into w_m026 from vb.f003_hist2 where m003=mnb_rec.torzsszam and datum=(select max(datum) from vb.f003_hist2 where m003=mnb_rec.torzsszam)
                    and rownum=1;
                exception
                    when no_data_found then
                        null;
                end;
                eddig:=20;				
                mehet:=false;
--megn�zz�k, mi�rt ker�lt a kurzorba a t�rzssz�m				
                begin
                    select count(*) into kulondb from vb_rep.vb_app_init where 
					param_nev='m003' 
					--and  program=programneve
					and m003=gszr_rec.m003  and to_date(param_dtol,'YYYY-MM-DD hh24:mi:ss')>=utolso_futas;
                exception
                    when others then
					    serr:=sqlcode;
                        kulondb:=0;
						--dbms_output.put_line('k�l�ndb exception :'||to_char(serr));
                end;
				--dbms_output.put_line(to_char(gszr_rec.m003)||':k�l�ndb:'||to_char(kulondb));
				eddig:=21;
				--dbms_output.put_line('van-e kulondb:'||to_char(kulondb));
                if kulondb!=0 or 
				   ((w_stathataly >= utolso_futas and w_statteaor!=mnb_rec.statteaor and w_kuldes_vege is null) or --a nemzeti sz�ml�s TE�OR az utols� fut�s �ta ker�lt be
                   (w_kuldes_vege>= utolso_futas)) then
                      mehet:=true;
					  EDDIG:=22;
                  --dbms_output.put_line('mehet1');                         
                else   
                  --dbms_output.put_line('mehet2');                    
                 -- nem �rbev�tel, l�tsz�m kat., vagy ESA k�d miatt ker�lt a kurzorba
				    eddig:=23;
                    begin      
                      --�s statisztika gy�jt�se					
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
						--dbms_output.put_line('StatTE�OR');
						if mnb_rec.MP65_r >= utolso_futas then  --m063_r
							MP65_r_db := MP65_r_db + 1; --m063_r_db:=m063_r_db+1;
                            if mnb_rec.m003_r_date >= utolso_futas then 
                                m003_r_db_for_MP65 := m003_r_db_for_MP65 + 1;  --m003_r_db_for_m063 := m003_r_db_for_m063 + 1; 
                            end if;
						end if;	
						--dbms_output.put_line('ESA');
						if mnb_rec.ueleszt_R>=utolso_futas then--Szil�gyi �d�m 2022.10.19. _R
							ueleszt_db:=ueleszt_db+1;
						end if;
						--dbms_output.put_line('c�gjegyz�k sz�m);
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
                    if (mnb_rec.m040 not in ('0','9') or substr(mnb_rec.m040k,1,4)=to_char(sysdate,'YYYY'))  -- �l�, vagy az adott napt�ri �vben sz�nt meg --substr(mnb_rec.m040k,1,1) volt substr(mnb_rec.m040k,1,4) helyett, de �gy csak az �vsz�m els� jegye volt hasonl�tva YYYY-al
                              or 
                            ( mnb_rec.m040 in ('0','9') and mnb_rec.m040k_r>=utolso_futas) or kulondb>0  then 
                            mehet:=true;
                            --dbms_output.put_line(mnb_rec.torzsszam||' mehet');	
	                else 
                             mehet:=false;
                              megszunt:=true;
                              --dbms_output.put_line(mnb_rec.torzsszam||' megpusztult,'||mnb_rec.m040k||' nem mehet');	
							 dbms_output.put_line('A k�vetkez� t�rzssz�m kihagyva, mert kor�bbi �vben (' || substr(mnb_rec.m040k, 1, 4) || '-' || substr(mnb_rec.m040k, 5, 2) || '-' ||  substr(mnb_rec.m040k, 7, 2) || ') sz�nt meg: ' || mnb_rec.torzsszam || '.');	
                    end if;--vagy, ha a megsz�n�si inform�ci� csak most ker�lt a regiszterbe.
					eddig:=26;
             
                    if not mehet and not megszunt then
                       
						eddig:=27;
                        if mnb_rec.letszam_h_date    >= utolso_futas and mnb_rec.m025!=nvl(w_m025,'00') then 
                            mehet:=true; 
							--dbms_output.put_line('letszam_h miatt mehet');							 
                        elsif mnb_rec.m025=nvl(w_m025,'00') then     
                            dbms_output.put_line('Nem v�ltozott:'||to_char(mnb_rec.torzsszam)||' m025:'||w_m025||'='||mnb_rec.m025);                       
                            nem_valtozott:=nem_valtozott+1;
                        end if;  
                        eddig:=28;						
                        if mnb_rec.arbev_r      >= utolso_futas and mnb_rec.m026!=nvl(w_m026,'0') then
                            mehet:=true; 
							--dbms_output.put_line('arbev_r miatt mehet');	
                        elsif mnb_rec.m026=nvl(w_m026,'0') then                       
                            dbms_output.put_line('Nem v�ltozott:'||to_char(mnb_rec.torzsszam)||' m026:'||w_m026||'='||mnb_rec.m026);  
                            nem_valtozott:=nem_valtozott+1;                                   
                        end if; 
						eddig:=29;
						--2019.09.06.  Levizsg�ljuk, hogy megv�ltozott-e az alakul�s d�tuma ut�lag, hogy ha m�s nem v�ltozott, akkor is benne maradjon a 
                        --           lev�logat�sban
						w_alakdat:=null;
						begin
							select alakdat_u into w_alakdat from vb.f003_hist3pr where
							m003=mnb_rec.ksh_torzsszam and datum>utolso_futas and alakdat!=alakdat_u;
						exception when no_data_found then
						     null;
						end;
						if w_alakdat is not null then 
						--megv�ltozott az alakul�s d�tuma, mindenhogyan �t kell adni!
						    mehet:=true;
						end if;	
                    end if;
                end if;
                --dbms_output.put_line('m026 a hist2-b�l');                   
                --dbms_output.put_line('Hat�rellen�rz�s j�n');                        
                 ---  K�ldhet� lenne, de valamelyik d�tum hib�s, ez�rt m�gsem k�ldhet�
                if  not to_date(mnb_rec.gfo_hataly,'YYYYMMDD')    between alsohatar and felsohatar  then 
                      mehet:=false; 
                      dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':m0491_h:'||mnb_rec.gfo_hataly); 
                end if;
				eddig:=30;
                 --dbms_output.put_line('m0491_h'); 
                if not to_date(mnb_rec.megyekod_hataly,'YYYYMMDD')  between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                      mehet:=false;
                      dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':m005_szh_h:'||mnb_rec.megyekod_hataly); 
                end if;
				eddig:=31;
                 --dbms_output.put_line('m005_szh_h'); 
                if not to_date(mnb_rec.nev_h,'YYYYMMDD')        between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                      mehet:=false;                 
                      dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':nev_h:'||mnb_rec.nev_h ); 
                 end if;
				 eddig:=32;
                 begin
				 --dbms_output.put_line('nev_h'); 
                if not nvl(to_date(mnb_rec.RNEV_H,'YYYYMMDD') ,sysdate)   between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                      mehet:=false;                   
                      dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':rnev_h:'||mnb_rec.RNEV_H); 
                 end if;
				 exception when others then
				     dbms_output.put_line(to_char( mnb_rec.torzsszam)||':nev_h:'); 
				 end;
				 eddig:=33;
                 --dbms_output.put_line('rnev_h'); 
                if not to_date(mnb_rec.SZEKHELY_H,'YYYYMMDD')    between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527')  then 
                       mehet:=false;  
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':SZEKHELY_H:'||mnb_rec.SZEKHELY_H); 
                 end if;
				 eddig:=34;
                 --dbms_output.put_line('szekhely_h'); 
                if not nvl(mnb_rec.LEVELEZESI_R_date,sysdate)  between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                       mehet:=false;  
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':LEVELEZESI_R:'||mnb_rec.LEVELEZESI_R); 
                 end if;
				 eddig:=35;
                 --dbms_output.put_line('levelezesi_r'); 
                if not nvl(mnb_rec.leV_PF_R_date,sysdate)   between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527') then 
                       mehet:=false;  
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':leV_PF_R :'||mnb_rec.leV_PF_R); 
                 end if;
				 eddig:=36;
                 --dbms_output.put_line('lev_pf_r'); 
                if not nvl(to_date(mnb_rec.mukodv,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':mukodv:'||mnb_rec.mukodv); 
                 end if;
				 eddig:=37;
                 --dbms_output.put_line('mukodv'); 
                if not nvl(to_date(mnb_rec.letszam_h,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':letszam_h:'||mnb_rec.letszam_h); 
                 end if;
				 eddig:=38;
                 --dbms_output.put_line('letszam_h'); 
                if not nvl(to_date(mnb_rec.arbev_h,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':arbev_h:'||mnb_rec.arbev_h); 
                 end if;
				 eddig:=39;
                 --dbms_output.put_line('arbev_h'); 
                if not to_date(mnb_rec.alakdat,'YYYYMMDD')  between alsohatar and felsohatar and mnb_rec.torzsszam not in ('15302724','15736527')  then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':alakdat:'||mnb_rec.alakdat); 
				elsif mnb_rec.torzsszam in ('15302724','15736527')  then 
				       mnb_rec.alakdat:='19830101';
                end if;
				 eddig:=40;
                 --dbms_output.put_line('alakdat'); 
                if not to_date(mnb_rec.m0781_h,'YYYYMMDD') between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':m0781_h:'||mnb_rec.m0781_h); 
                 end if;
				 eddig:=41;
                --dbms_output.put_line('m0781_h'); 
				--dbms_output.put_line('stathat�ly:'||mnb_rec.stathataly);
                if not mnb_rec.stathataly  between to_char(alsohatar,'YYYYMMDD') and to_char(felsohatar,'YYYYMMDD') then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||':m0581_h:'||mnb_rec.stathataly); 
                 end if;
				 eddig:=42;
                 --dbms_output.put_line('m0581_h'); 
                if not nvl(to_date(mnb_rec.MP65_h,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then   --m063_h
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum: '||to_char(mnb_rec.torzsszam) || ' :mp65_h: ' || mnb_rec.MP65_h);  --mnb_rec.m063_h
                 end if;
				 eddig:=43;
                 --dbms_output.put_line('m063_h'); 
                if not nvl(to_date(mnb_rec.ueleszt,'YYYYMMDD'),sysdate)   between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum: '||to_char(mnb_rec.torzsszam)||'ueleszt: '||mnb_rec.ueleszt); 
                 end if;
				 eddig:=44;
                 --dbms_output.put_line('ueleszt'); 
                if not to_date(mnb_rec.m003_r,'YYYYMMDD') between alsohatar and felsohatar then 
                       mehet:=false;                   
                       dbms_output.put_line('Hib�s d�tum:'||to_char(mnb_rec.torzsszam)||'m003_r :'||mnb_rec.m003_r); 
                 end if;
				 eddig:=45;
                 --dbms_output.put_line('m003_r'); 
                 --dbms_output.put_line('Hat�rellen�rz�s v�ge');      
           -- $if $$debug_on $then
			  -- dbms_output.put_line(mnb_rec.torzsszam||' '||case when mehet then 'mehet' else 'nem mehet' end);
			--$end
            --dbms_output.put_line('D�tumhib�k ellen�rizve');                  
            if mehet then
                sor:=mnb_rec.q01||','||                           -- adatgy�jt�s k�dja
                    mnb_rec.datum1||','||                         -- vonatkoz�si id� 8 hosszon
                    mnb_rec.ksh_torzsszam||','||                  -- KSH t�rzssz�ma
                    mnb_rec.kitoltes_datum||','||                 -- kit�lt�s d�tuma 8 hosszon
                    mnb_rec.kshtorzs||                            -- 1 karakter fixen: E, t�blak�d: @KSHTORZS
                    mnb_rec.rekordsorszam||','||                  -- sorsz�m 7 karakteren el�null�zva  a kurzor rekordsz�m�b�l levonva az eddig �tl�pett rekordok sz�m�t
                    mnb_rec.torzsszam||','||                      -- t�rzsszam
                    mnb_rec.gfo||','||                            -- GFO
                    mnb_rec.gfo_hataly||','||                     -- GFO hat�ly
                    mnb_rec.megyekod||','||                       --sz�khely megyek�dja, 
                    mnb_rec.megyekod_hataly||','||                --a megyek�d hat�ly d�tuma
                    mnb_rec.nev||','||                            --n�v
                    mnb_rec.nev_h||','||                          --n�v hat�lya
                    mnb_rec.rnev||','||                           --r�vid n�v
                    mnb_rec.RNEV_H||','||                         --r�vid n�v hat�lya
                    mnb_rec.M054_SZH||','||                       --sz�khely ir�ny�t� sz�m
                    mnb_rec.telnev_szh||','||                     --sz�khely	
                    mnb_rec.utca_szh||','||                       --sz�khely utca, h�zsz�m
                    mnb_rec.SZEKHELY_H||','||                     --sz�khely c�m hat�lya 	
                    mnb_rec.M054_LEV||','||	                      --levelez�si c�m ir�ny�t� sz�ma
                    mnb_rec.telnev_lev||','||                     --levelez�si c�m telep�l�s n�v	
                    mnb_rec.utca_lev||','||             --levelez�si c�m utca	
                    mnb_rec.LEVELEZESI_R||','||         --levelez�si c�m rendszerbe ker�l�se	
                    mnb_rec.m054_pf_lev||','||          --postafi�kos levelez�si c�m ir�ny�t� sz�ma 
                    mnb_rec.telnev_pf_lev||','||        --postafi�kos levelez�si c�m telep�l�s neve
                    mnb_rec.pfiok_lev||','||            --postafi�k
                    mnb_rec.leV_PF_R||','||             --pf. c�m hat�lya (rendszerbe ker�l�se)
                    mnb_rec.M040||','||                 --m�k�d�s �llapotk�dja �tk�dolva: 0,9->0, egy�bk�nt 1               
                    mnb_rec.m040k||','||                --�llapotk�d hat�lya
                    mnb_rec.m025||','||                 --l�tsz�m kateg�ria
                    mnb_rec.letszam_h||','||            --l�tsz�m kateg�ria besorol�s d�tuma
                    mnb_rec.m026||','||                 --�rbev�tel kateg�ria
                    mnb_rec.arbev_h||','||              --�rbev�tel kateg�ria hat�lya
                    mnb_rec.m009_szh||','||             --sz�khely telep�l�s k�d+cdv 5 jegyen					
                    mnb_rec.alakdat||','||              --alakul�s d�tuma
                    mnb_rec.m0781||','||                --admin szak�g 2008
                    mnb_rec.m0781_h||','||              --2008-as besorol�si k�d hat�lya
                    mnb_rec.m058_j||','||               --janus TE�OR
                    mnb_rec.m0581_j_h||','||            --janus TE�OR hat�lya (azonos a stat TE�OR hat�ly�val)
                    mnb_rec.MP65||','||                 --ESA szektork�d   --mnb_rec.m063
                    mnb_rec.MP65_H||','||               --ESA szektork�d hat�lya   -mnb_rec.m063_h
                    mnb_rec.ueleszt ||','||              --�jra�leszt�s hat�lya
                    mnb_rec.m003_r||','||               --szervezet rendszerbe ker�l�se
                    mnb_rec.datum2||','||               --napi lev�logat�s d�tuma
                    mnb_rec.statteaor||','||            --statisztikai TE�OR
                    mnb_rec.stathataly||','||           --statisztikai TE�OR hat�lya		
                    mnb_rec.cegjegyz||','||             --c�gjegyz�k sz�m
					mnb_rec.cegjegyz_h||','||           --c�gjegyz�ksz�m hat�lya
					mnb_rec.mvb39||','||                --IFRS nyilatkozat
					mnb_rec.mvb39_h||','||              --IFRS nyilatkozat hat�lya
                    mnb_rec.ORSZ || ',' ||              --Orsz�gk�d
                    mnb_rec.LETSZAM || ',' ||           --L�tsz�m
                    mnb_rec.ARBEV || ',' ||             --�rbev�tel 
                    mnb_rec.M0783 || ',' ||             --Adminisztrat�v TE�OR 2025
                    mnb_rec.M0783_H || ',' ||           --2025-�s besorol�si k�d hat�lya
                    mnb_rec.M0583 || ',' ||             --Statisztikai TE�OR 2025
                    mnb_rec.M0583_H || ',' ||           --2025-�s besorol�si k�d hat�lya
                    mnb_rec.stat_szakag_t�pus || ',' || --0 = Statisztikai TE�OR, 1 = nemzeti sz�ml�s TE�OR
                    mnb_rec.EMAIL || ',' ||             --E-mail c�m
                    mnb_rec.EMAIL_H || ',' ||           --E-mail c�m hat�ly kezdete
                    mnb_rec.Cegkapu || ',' ||           --C�gkapu
                    mnb_rec.Cegkapu_H;                  --C�gkapu hat�ly kezdete;                      
                    
                    eddig:=46;					
               --t�bl�ba sz�rjuk a k�ldend� rekordot
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
                                                values(serr,'mnb napi adatk�ld�:'||programneve||datum_kar||':'||to_char(mnb_rec.torzsszam),errmsg);
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
                       vb.mod_szam_tolt('K865', tablaneve, sorok, 'MNB napi v�ltoz�slista k�ld�se, debug m�d ', programneve || verzio, datum, 'M');     
                   $else
				       vb.mod_szam_tolt('K865', tablaneve, sorok, 'MNB napi v�ltoz�slista k�ld�se', programneve || verzio, datum, 'M');
				   $end
                   exception when others then
                       serr:=sqlcode;                   
                       insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: ','mod_szam_tolt a hiba');
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
		and substr(rekord,5,8)=to_char(sysdate,'YYYYMMDD');-- and substr(rekord, 32, 1) != 'N';--Szil�gyi �d�m 2022.10.21.
		eddig:=51;
        if(levalogatva_1 != 0) then
            dbms_output.put_line('Most lev�logatott Q01-es sorok: ' || levalogatva_1);
       -- else 
       --     dbms_output.put_line('A mai nap folyam�n nem ker�lt Q01-es adat lev�logat�sra.');
        end if;
		if puffersorok>0 then 
		    dbms_output.put_line('Puffer sorok sz�ma:  '||puffersorok);
        end if;		
        if(kihagyott > 0) then 
            dbms_output.put_line('Kihagyott sorok: ' || kihagyott || ', amelyekb�l nem v�ltozott: ' || nem_valtozott || '.');
        end if;
        --dbms_output.put_line('ebb�l: nem v�ltozott:'||nem_valtozott);  
		
		--Statisztik�k:
		dbms_output.put_line('');
    	select count(*) into osszesq01 from $if $$debug_on $then vb_rep.mnb_napi_debug $else vb_rep.mnb_napi $end where kod='Q01';
		--dbms_output.put_line('A t�bl�ban tal�lhat� Q01-es c�gek sz�ma: ' || to_char(osszesq01) || '.');
        dbms_output.put_line('---');
        --dbms_output.new_line;
        if(levalogatva_1 != 0) then
            dbms_output.put_line('�tfed� statisztik�k, amelyekb�l a m�sodik sz�m az �rintett �j t�rzssz�mok sz�m�val cs�kkentett sz�m: ');
            --dbms_output.put_line('A m�sodik sz�m az �j t�rzssz�mok n�lk�li v�ltoz�sok sz�ma');
            --dbms_output.put_line('');
            dbms_output.new_line;
            if m003_r_db>0 then 
                dbms_output.put_line('�j t�rzssz�m (M003): ' ||to_char(m003_r_db) || '.');
            end if;
            if m0491_r_db>0 then 
                dbms_output.put_line('GFO v�ltoz�s (M0491): ' ||to_char(m0491_r_db)|| ' : ' ||to_char(m0491_r_db-least(m003_r_db,m0491_r_db)) || '.');		
            end if;
            if m005_szh_db>0 then 
                dbms_output.put_line('Megyek�d v�ltoz�s (M005_SZH): '||to_char(m005_szh_db)||' : '||to_char(m005_szh_db-least(m003_r_db,m005_szh_db)) || '.');				
            end if;	
            if nev_r_db>0 then 
                dbms_output.put_line('N�v v�ltoz�s (NEV): '||to_char(nev_r_db)||' : '||to_char(nev_r_db-least(m003_r_db,nev_r_db)) || '.');		
            end if;
            if rnev_r_db>0 and m003_r_db_for_rnev = 0 then 
                dbms_output.put_line('R�vid n�v v�ltoz�s (RNEV): ' || to_char(rnev_r_db) || '.');-- ||' : '||to_char(rnev_r_db-least(m003_r_db,rnev_r_db)) || '.');		
            elsif rnev_r_db>0 and m003_r_db_for_rnev > 0 then
                dbms_output.put_line('R�vid n�v v�ltoz�s (RNEV): ' || to_char(rnev_r_db) || ' : ' || to_char(rnev_r_db - m003_r_db_for_rnev) || '.');		
            end if;
            if szekhely_r_db>0 then 
                dbms_output.put_line('Sz�khely v�ltoz�s (SZEKHELY): '||to_char(szekhely_r_db)||' : '||to_char(szekhely_r_db-least(m003_r_db,szekhely_r_db)) || '.');		
            end if;
            if levelezesi_r_db>0 and m003_r_db_for_levelezesi = 0 then 
                dbms_output.put_line('Levelez�si c�m v�ltoz�s (LEVELEZESI): ' || to_char(levelezesi_r_db) || '.');--||' : '||to_char(levelezesi_r_db-least(m003_r_db,levelezesi_r_db)) || '.');
            elsif levelezesi_r_db>0 and m003_r_db_for_levelezesi > 0 then
                dbms_output.put_line('Levelez�si c�m v�ltoz�s (LEVELEZESI): ' || to_char(levelezesi_r_db) || ' : ' || to_char(levelezesi_r_db - m003_r_db_for_levelezesi) || '.');
            end if;
            if LEV_PF_R_db>0 and m003_r_db_for_lev_pf = 0 then 
                dbms_output.put_line('Postafi�k v�ltoz�s (LEV_PF): ' || to_char(lev_pf_r_db) || '.');-- ||' : '||to_char(lev_pf_r_db-least(m003_r_db,lev_pf_r_db)) || '.');		
            elsif LEV_PF_R_db>0 and m003_r_db_for_lev_pf > 0 then
                dbms_output.put_line('Postafi�k v�ltoz�s (LEV_PF): ' || to_char(lev_pf_r_db) || ' : ' || to_char(lev_pf_r_db - m003_r_db_for_lev_pf) || '.');
            end if;
            if m040k_r_db>0 then 
                dbms_output.put_line('�llapotk�d v�ltoz�s (M040K): '||to_char(m040k_r_db)||' : '||to_char(m040k_r_db-least(m003_r_db,m040k_r_db)) || '.');				
            end if;
            if m040v_r_db>0 then 
                dbms_output.put_line('�llapotk�d v�ge (M040V): ' || to_char(m040v_r_db) || '.');-- || ' : ' || to_char(m040v_r_db-least(m003_r_db,m040v_r_db)) || '.');
            end if;
            if m0783_r_db >0 then 
                dbms_output.put_line('Adminisztrat�v TE�OR v�ltoz�s (M0783): '||to_char(m0783_r_db)||' : '||to_char(m0783_r_db-least(m003_r_db, m0783_r_db)) || '.');
            end if;
            if letszam_h_db>0 then
                dbms_output.put_line('L�tsz�m kateg�ria v�ltoz�s (LETSZAM): '||to_char(letszam_h_db)||' : '||to_char(letszam_h_db-m003_r_db) || '.');
            end if;	
            if arbev_r_db>0 then
                dbms_output.put_line('�rbev�tel-kateg�ria v�ltoz�s (ARBEV): '||to_char(arbev_r_db)||' : '||to_char(arbev_r_db-least(m003_r_db,arbev_r_db)) || '.');		
            end if;
            if m0583_r_db >0 then
                dbms_output.put_line('Statisztikai TE�OR v�ltoz�s (M0583): '||to_char(m0583_r_db)||' : '||to_char(m0583_r_db-least(m003_r_db, m0583_r_db)) || '.');		
            end if;
            /*if m063_r_db>0 and m003_r_db_for_m063 = 0 then
                dbms_output.put_line('ESA szektork�d v�ltoz�s (M063): ' || to_char(m063_r_db) || '.');		--||' : '||to_char(m063_r_db-least(m003_r_db,m063_r_db))	
            elsif m063_r_db>0 and m003_r_db_for_m063 > 0 then
                dbms_output.put_line('ESA szektork�d v�ltoz�s (M063): ' || to_char(m063_r_db) || ' : ' || to_char(m063_r_db - m003_r_db_for_m063) || '.');		
            end if;	*/
            if MP65_r_db > 0 and m003_r_db_for_MP65 = 0 then
                dbms_output.put_line('ESA szektork�d v�ltoz�s (MP65): ' || to_char(MP65_r_db) || '.');		--||' : '||to_char(m063_r_db-least(m003_r_db,m063_r_db))	
            elsif MP65_r_db > 0 and m003_r_db_for_MP65 > 0 then
                dbms_output.put_line('ESA szektork�d v�ltoz�s (MP65): ' || to_char(MP65_r_db) || ' : ' || to_char(MP65_r_db - m003_r_db_for_MP65) || '.');		
            end if;	
            if ueleszt_db>0 then
                dbms_output.put_line('�jra�leszt�sek (UELESZT): '||to_char(ueleszt_db) || '.');
            end if;
            if ifrs_db>0 then
                dbms_output.put_line('�j IFRS nyilatkozatok (MVB39): '||to_char(ifrs_db) || '.');
            end if;	
            if cegv_db>0 and m003_r_db_for_cegv = 0 then
                dbms_output.put_line('C�gjegyz�ksz�m v�ltoz�sok (CEGV): '||to_char(cegv_db) || '.');
            elsif cegv_db>0 and m003_r_db_for_cegv > 0 then
                dbms_output.put_line('C�gjegyz�ksz�m v�ltoz�sok (CEGV): ' || to_char(cegv_db) || ' : ' || to_char(cegv_db - m003_r_db_for_cegv) || '.');
            end if;
            eddig:=52;      
        else
            dbms_output.put_line('A mai nap folyam�n nem ker�lt Q01-es c�g lev�logat�sra.');
        end if;
        --insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: ','6. Els� mell�klet k�sz'||to_char(sysdate,'YYYYMMDD HH24:MI:SS'));
--Ha nincsen Q01 sor (mert pl kiesett mind a ciklus belsej�ben!)
--Ha az elej�n m�r betettem a nemleges sort, akkor 1 db lesz, azaz itt nem ker�l be m�g egyszer.
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
                        where c.m003=f003_juje.m003_je and to_date(param_dtol,'YYYY-MM-DD hh24:mi:ss')>=utolso_futas  -- �l�, vagy az adott napt�ri �vben sz�nt meg
                union   --a jogut�d n�lk�l megsz�nt, de k�lf�ldi jogut�dos szervezeteket is hozz� teszi a jeju rekordokhoz
                        select m003_je,'00000001' m003_ju,null mv07,					   
						decode(j.atalak,'A','220','B','230','E','235','K','830','L','835','O','240','S','285','V','280','990') mv501_je,
					    decode(j.atalak,'A','120','B','930','E','190','K','130','L','135','O','140','S','185','V','180','990') mv501_ju,
					    g.m040k dtol,j.datum_r,kulf_ju, null orszagkod,'0' from vb_ceg.jogutod j, vb.f003 g
                        where kulf_ju='1' and g.m003=m003_je and g.m040k_r>=utolso_futas and g.m040='9'
				union  -- a vb_app_init-be f�lvett t�rzssz�mok k�z�l a k�lf�ldi jogut�dos megsz�n�sek
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
					insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||programneve||datum_kar,errmsg);
					commit;
			end;  
        end loop;
		commit; 
		select count(*) into levalogatva_2 from  $if $$debug_on $then vb_rep.mnb_napi_debug  $else vb_rep.mnb_napi $end  where kod='Q02' and 
		substr(rekord,5,8)=to_char(sysdate,'YYYYMMDD');
		eddig:=57;
		if levalogatva_2=0 then          
	  --nincs m�sodik mell�klet
			begin
				sor:='Q02,'||to_char(sysdate,'YYYYMMDD')||',15302724,'||to_char(sysdate,'YYYYMMDD')||',N';
				insert into $if $$debug_on $then vb_rep.mnb_napi_debug  $else vb_rep.mnb_napi $end
						values ('Q02',q02mellekletnev,1,sor);
				levalogatva_2:=0;--1 helyett nulla		
			exception
				when others then
					  serr:=sqlcode;
					  errmsg:=substr(sqlerrm,1,100);
					  insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||programneve||datum_kar,errmsg);
			end;         
			commit;
		end if;  --q02db+sorok1
		eddig:=58;
        dbms_output.put_line('---');
        if(levalogatva_2 != 0) then
            dbms_output.put_line('Lev�logatott Q02-es jogel�d-jogut�d c�gp�rok: ' || to_char(levalogatva_2) || ', amelyek k�z�l k�lf�ldi jogut�d: ' || to_char(kulf_db) || '.');	
        else
            dbms_output.put_line('A mai nap folyam�n nem ker�lt Q02-es c�gp�r lev�logat�sra.');
        end if;
        
		begin
			update vb_rep.vb_app_init 
				 set 
				 param_ertek=datum_kar  --az utols� fut�s d�tuma
			where 
				 alkalmazas='MNB napi v�ltoz�slista k�ld�se' 
			and  program='mnb_EBEAD.sql' 
			and  $if $$debug_on $then param_nev='utolso_futas - debug' $else param_nev='utolso_futas' $end
			and   sysdate>to_date(param_dtol,'YYYY-MM-DD HH24:MI:SS'); 
			--update vb_rep.vb_app_init set m003=null;
			commit;
		exception when others then
			serr:=sqlcode;
			insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||programneve||datum_kar,'app_init_update a hiba');
		end;
		rekordsorszam:=rekordsorszam-kihagyott;
		eddig:=59;
		begin
		   $if $$debug_on $then
			vb.mod_szam_tolt('K865', tablaneve, rekordsorszam, 'MNB napi v�ltoz�slista k�ld�se, debug m�d  Kihagyott: ' || to_char(kihagyott), programneve || verzio, datum, 'V');
		   $else
		     vb.mod_szam_tolt('K865', tablaneve, rekordsorszam, 'MNB napi v�ltoz�slista k�ld�se  Kihagyott: ' || to_char(kihagyott), programneve || verzio, datum, 'V');
           $end		   
		exception when others then
			insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||datum_kar,'mod_szam_tolt a hiba');
		end;
		eddig:=60;
		commit;
exception
    when others then
        serr:=sqlcode;
        insert into vb.vb_uzenet values (serr,'mnb napi adatk�ld�: '||programneve||datum_kar,'A program eg�sz�t �rint� hiba: valami lev�logat�si hiba van a '||to_char(eddig)||' sz�mn�l a'||to_char(w_m003)||' t�rzssz�mon');
        commit;
        dbms_output.put_line(to_char(serr)||'  valami lev�logat�si hiba t�rt�nt '||to_char(eddig)||' sz�mn�l a'||to_char(w_m003)||' t�rzssz�mon');
        $if $$debug_on $then
		   vb.mod_szam_tolt('K865', tablaneve, rekordsorszam, 'DEBUG M�dban HIB�VAL �RT V�GET ' || to_char(eddig) || ' sz�mn�l a ' || to_char(w_m003) || ' t�rzssz�mon:' || to_char(serr), programneve || verzio, datum, 'V');
        $else
		   vb.mod_szam_tolt('K865', tablaneve, rekordsorszam, 'HIB�VAL �RT V�GET ' || to_char(eddig) || ' sz�mn�l a ' || to_char(w_m003) || ' t�rzssz�mon:' || to_char(serr), programneve || verzio, datum, 'V');
		$end
		commit;
end;
/                                                       