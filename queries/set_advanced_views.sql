


create view caf_vikt_outerjoined as (
    select
    case when tbl2.ids__uid notnull then tbl2.ids__uid else tbl1.ids__uid end as "ids__uid",
    case when tbl2.ids__interval notnull then tbl2.ids__interval else tbl1.ids__interval end as "ids__interval",
    case when tbl2.interval__start notnull then tbl2.interval__start else tbl1.interval__start end as "interval__start",
    case when tbl2.interval__end notnull then tbl2.interval__end else tbl1.interval__end end as "interval__end",
    cirk_vikt,
    tbl1."caffein"
    from (
        select ids__uid, ids__interval, interval__start, interval__end, cirk_vikt
        from vikt
        ) tbl2
    full outer join (select ids__uid, ids__interval, interval__start, interval__end, coalesce(med.lm_givet__peyona__inj_inf__20__mg_ml_mg,
	    med.lm_givet__peyona__oralt__20__mg_ml_mg,
	    med.lm_givet__peyona__oralt__spadning__till__10__mg_ml_mg,
	    med.lm_givet__peyona__spadning__till__10__mg_ml__inf_mg) "caffein" from med) tbl1
    using (ids__interval, interval__start, interval__end, ids__uid)
);



create view view__caffein_uid_has_validloadingdose as (
    select ids__uid
    from (
        select ids__uid,
        split_part(string_agg(value::varchar,'__'),'__',1)::numeric "first_dose",
        case when split_part(string_agg(value::varchar,'__'),'__',2) like '' then NULL else split_part(string_agg(value::varchar,'__'),'__',2)::numeric end as "second_dose"
        from (
            select ids__uid, birthdate,
                (split_part(caffein,'__',2)::timestamp - birthdate) as "pna",
                split_part(caffein,'__',1)::numeric "value",
                split_part(caffein,'__',2)::timestamp "datetime"
                from (
                    select overview.ids__uid,
                        overview.birthdate,
                        caf_vikt_outerjoined.interval__start,
                        caf_vikt_outerjoined.interval__end,
                        caf_vikt_outerjoined."caffein"
                        from overview, caf_vikt_outerjoined
                        where overview.ids__uid in (select * from view__med_uid_has_caffein vmuhc)
                        and caf_vikt_outerjoined.ids__uid = overview.ids__uid
                        order by (caf_vikt_outerjoined.ids__uid, caf_vikt_outerjoined.interval__start)
                ) foo
                where caffein notnull
                and (split_part(caffein,'__',2)::timestamp - birthdate) < '14 days'::interval
                order by (ids__uid,(split_part(caffein,'__',2)::timestamp - birthdate))
        ) foo2
        group by ids__uid
    ) foo3
     where first_dose >= 1.3*second_dose
);