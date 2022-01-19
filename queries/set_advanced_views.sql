
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
                        med.interval__start,
                        med.interval__end,
                        coalesce(med.lm_givet__peyona__inj_inf__20__mg_ml_mg,
                        med.lm_givet__peyona__oralt__20__mg_ml_mg,
                        med.lm_givet__peyona__oralt__spadning__till__10__mg_ml_mg,
                        med.lm_givet__peyona__spadning__till__10__mg_ml__inf_mg) "caffein"
                        from med,overview,vikt
                        where overview.ids__uid in (select * from view__med_uid_has_caffein vmuhc)
                        and med.ids__uid = overview.ids__uid
                        and med.ids__uid = vikt.ids__uid
                        and med.ids__interval = vikt.ids__interval
                        order by (med.ids__uid,med.interval__start)
                ) foo
                where caffein notnull
                and (split_part(caffein,'__',2)::timestamp - birthdate) < '14 days'::interval
                order by (ids__uid,(split_part(caffein,'__',2)::timestamp - birthdate))
        ) foo2
        group by ids__uid
    ) foo3
     where first_dose >= 1.3*second_dose
);