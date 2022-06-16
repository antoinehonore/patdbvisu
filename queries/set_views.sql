create view view__interv_all as (
	select ids__uid,ids__interval,interval__start,interval__end from monitorhf
	union
	select ids__uid,ids__interval,interval__start,interval__end from monitorlf
	union
	select ids__uid,ids__interval,interval__start,interval__end from vikt
	union
	select ids__uid,ids__interval,interval__start,interval__end from vatska
	union
	select ids__uid,ids__interval,interval__start,interval__end from takecare
	union
	select ids__uid,ids__interval,interval__start,interval__end from respirator
	union
	select ids__uid,ids__interval,interval__start,interval__end from pressure
	union
	select ids__uid,ids__interval,interval__start,interval__end from med
	union
	select ids__uid,ids__interval,interval__start,interval__end from lab
	union
	select ids__uid,ids__interval,interval__start,interval__end from fio2
);

create view view__uid_all as (
    select distinct ids__uid from view__interv_all
    union
    select ids__uid from overview
);


create view view__length_of_stay as (
	select ids__uid,
	count(ids__interval)*2 as "n_days",
	min(interval__start) as "interval__start",
	max(interval__end) as "interval__end"
	from view__interv_all
	group by ids__uid
);


create view view__monitorlf_has_btb as (
    select ids__uid ,ids__interval
    from monitorlf ddwl
    where ddwl."lf__147840__147850__147850__btbhf__btbhf__spm" notnull or ddwl."lf__none__147850__none__btbhf__none__spm" notnull
);

create view view__monitorlf_uid_has_btb as
(
	select distinct ids__uid
	from view__monitorlf_has_btb
);

create view view__monitorlf_has_rf as (
    select ids__uid, ids__interval
    from monitorlf ddwl
    where   ddwl."lf__151562__151562__151562__rf__rf__rpm" notnull or
            ddwl."lf__none__151562__none__rf__none__rpm" notnull or
            ddwl."lf__151562__151562__151562__rf__rf__ok-nd" notnull or
            ddwl."lf__none__151562__none__rf__none__okand" notnull
);

create view view__monitorlf_uid_has_rf as
(
	select distinct ids__uid
	from view__monitorlf_has_rf
);

create view view__monitorlf_has_spo2 as (
select ids__uid ,ids__interval
from monitorlf ddwl
where
ddwl."lf__150456__150456__150456__spo-__spo-__perc" notnull
				    or ddwl."lf__none__150456__none__spo2__none__perc" notnull or ddwl."lf__150456__150456__150472__spo-__spo-__perc" notnull
				    or ddwl."lf__150456__150456__150476__spo-__spo-__perc" notnull or ddwl."lf__150456__150456__192960__spo-__spo-__perc" notnull
				    or ddwl."lf__150456__150456__192980__spo-__spo-__perc" notnull or ddwl."lf__none__150456__none__spo2h__none__perc" notnull
				    or ddwl."lf__150456__150456__150456__spo-__spo-__ok-nd" notnull
);

create view view__monitorlf_uid_has_spo2 as
(
	select distinct ids__uid
	from view__monitorlf_has_spo2
);



create view view__monitorlf_has_arts as (
select ids__uid ,ids__interval
from monitorlf ddwl
where
ddwl."lf__150016__150032__150033__art__arts__mmhg" notnull or
ddwl."lf__150032__150032__150033__art__arts__mmhg" notnull or
ddwl."lf__150016__150036__150033__abp__arts__mmhg" notnull or
ddwl."lf__none__150033__none__arts__none__mmhg" notnull
);

create view view__monitorlf_uid_has_arts as
(
	select distinct ids__uid
	from view__monitorlf_has_arts
);

create view view__monitorlf_has_onesignal as (
    select * from view__monitorlf_has_spo2
    union
    select * from view__monitorlf_has_rf
    union
    select * from view__monitorlf_has_btb
);

create view view__monitorlf_uid_has_onesignal as (
    select distinct ids__uid from view__monitorlf_has_onesignal
);


create view view__monitorlf_has_allsignals as (
    select * from view__monitorlf_has_spo2
    intersect
    select * from view__monitorlf_has_rf
    intersect
    select * from view__monitorlf_has_btb
);

create view view__monitorlf_uid_has_allsignals as (
    select distinct ids__uid
    from view__monitorlf_has_allsignals
);

create view view__overview_uid_neo as (
    select distinct ids__uid from overview where projid like '%%neo%%'
);

create view view__overview_uid_covid19 as (
    select distinct ids__uid from overview where projid like '%%covid19%%'
);

create view view__monitor_meta_uid_rsv2021 as (
    select distinct ids__uid from monitor_meta where monid like '%%2021__RSV%%'
);

create view view__monitor_meta_uid_rsv2022 as (
    select distinct ids__uid from monitor_meta where monid like '%%2022__RSV%%'
);
create view view__monitor_meta_uid_rsv as (
    select distinct ids__uid from monitor_meta where monid like '%%RSV%%'
);

create view view__overview_uid_u71_main as (
    select distinct ids__uid from overview where u071_main like 'yes'
);

create view view__overview_uid_u72_main as (
    select distinct ids__uid from overview where u072_main like 'yes'
);

create view view__overview_uid_either_u71_u72 as (
    select * from view__overview_uid_u71_main
    union
    select * from view__overview_uid_u72_main
);

create view view__overview_uid_covid_died as (
    select distinct ids__uid from overview where avlidentid notnull
);

create view view__overview_uid_vlbw as (
    select distinct ids__uid from overview where bw < 1500
);

create view view__overview_uid_preterm as (
    select distinct ids__uid from overview where ga_w <= 37
);

create view view__overview_uid_term as (
    select distinct ids__uid from overview where ga_w > 37
);

create view view__overview_uid_extremely_preterm as (
    select distinct ids__uid from overview where ga_w < 28
);

create view view__overview_uid_very_preterm as (
    select distinct ids__uid from overview where ga_w >= 28 and ga_w < 32
);

create view view__overview_uid_late_preterm as (
    select distinct ids__uid from overview where ga_w >= 32 and ga_w < 37
);

create view view__overview_uid_has as
(
	select distinct ids__uid
	from overview
);

create view view__monitorlf_has as
(
	select ids__uid ,ids__interval
	from monitorlf
);

create view view__monitorhf_has as
(
	select ids__uid ,ids__interval
	from monitorhf
);


create view view__takecare_has as
(
	select ids__uid ,ids__interval
	from takecare
	where extra notnull
);

create view view__takecare_uid_has as
(
	select distinct ids__uid
	from takecare
	where extra notnull
);

create view view__monitor_data_size as (
    select mlf.ids__interval,mlf.interval__start, pg_column_size(mlf) +pg_column_size(mhf) "datasize"
    from monitorlf mlf, monitorhf mhf
    where mlf.ids__interval =mhf.ids__interval
);

create view view__vikt_has as
(
	select ids__uid ,ids__interval
	from vikt
	where cirk_vikt notnull
);

create view view__vikt_uid_has as
(
	select distinct ids__uid
	from view__vikt_has
);


create view view__med_has_caffein as (
    select ids__uid,ids__interval
    from med
    where
    "lm_givet__peyona__inj_inf__20__mg_ml_mg" notnull or
    "lm_givet__peyona__oralt__20__mg_ml_mg"  notnull or
    "lm_givet__peyona__oralt__spadning__till__10__mg_ml_mg" notnull or
    "lm_givet__peyona__spadning__till__10__mg_ml__inf_mg" notnull
);

create view view__med_uid_has_caffein as
(
	select distinct ids__uid
	from view__med_has_caffein
);




create view view__clinisoft_start_end as (
	select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"
	from
	(
		select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"from med group by ids__uid
		union
		select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"from vatska group by ids__uid
		union
		select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"from vikt group by ids__uid
		union
		select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"from respirator group by ids__uid
		union
		select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"from pressure group by ids__uid
		union
		select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"from lab group by ids__uid
		union
		select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"from fio2 group by ids__uid
	) as foo
	group by ids__uid
	order by interval__start
);


create view view__clinisoft_has as (
    select ids__uid,ids__interval from med
    union
    select ids__uid,ids__interval from vatska
    union
    select ids__uid,ids__interval from vikt
    union
    select ids__uid,ids__interval from respirator
    union
    select ids__uid,ids__interval from pressure
    union
    select ids__uid,ids__interval from lab
    union
    select ids__uid,ids__interval from fio2
);

create view view__clinisoft_uid_has as (
    select distinct ids__uid from view__clinisoft_has
);


create view view__clinisoft_total_n_patients as
(
SELECT foo.interval__start as "interval__start", sum(n_patients) OVER (rows between unbounded preceding and current row) AS "total_n_patients__clinisoft"
FROM (
select count(ids__uid) as "n_patients", interval__start from view__clinisoft_start_end vtse
group by interval__start) as foo
ORDER  BY foo."interval__start"
);



create view view__takecare_start_end as (
	select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"
	from takecare
	group by ids__uid
	order by interval__start
);

create view view__takecare_total_n_patients as
(
SELECT foo.interval__start as "interval__start", sum(n_patients) OVER (rows between unbounded preceding and current row) AS "total_n_patients__takecare"
FROM (
select count(ids__uid) as "n_patients", interval__start from view__takecare_start_end vtse
group by interval__start) as foo
ORDER  BY foo."interval__start"
);


create view view__monitorlf_uid_has as (
select distinct ids__uid from monitorlf
);


create view view__monitorlf_start_end as (
	select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"
	from monitorlf
	group by ids__uid
	order by interval__start
);

create view view__monitorlf_total_n_patients as
(
SELECT foo.interval__start as "interval__start", sum(n_patients) OVER (rows between unbounded preceding and current row) AS "total_n_patients__monitorlf"
FROM (
select count(ids__uid) as "n_patients", interval__start from view__monitorlf_start_end vtse
group by interval__start) as foo
ORDER  BY foo."interval__start"
);

create view view__monitorhf_uid_has as (
    select distinct ids__uid from monitorhf
);

create view view__monitorhf_start_end as (
	select ids__uid, min(interval__start) as "interval__start", max(interval__end) as "interval__end"
	from monitorhf
	group by ids__uid
	order by interval__start
);

create view view__monitorhf_total_n_patients as
(
SELECT foo.interval__start as "interval__start", sum(n_patients) OVER (rows between unbounded preceding and current row) AS "total_n_patients__monitorhf"
FROM (
select count(ids__uid) as "n_patients", interval__start from view__monitorhf_start_end vtse
group by interval__start) as foo
ORDER  BY foo."interval__start"
);




create view view__timeline_n_patients as (
	select coalesce(a.interval__start,b.interval__start) as "interval__start", total_n_patients__takecare, total_n_patients__clinisoft, total_n_patients__monitorlf, total_n_patients__monitorhf
	from
	(
	select coalesce(c.interval__start, t.interval__start) as "interval__start", total_n_patients__takecare, total_n_patients__clinisoft
	from view__clinisoft_total_n_patients c
	full outer join view__takecare_total_n_patients t
	on c.interval__start = t.interval__start
	) as a
	full outer join
	(
	select coalesce(mlf.interval__start, mhf.interval__start) as "interval__start", total_n_patients__monitorlf, total_n_patients__monitorhf
	from view__monitorhf_total_n_patients mhf
	full outer join view__monitorlf_total_n_patients mlf
	on mhf.interval__start = mlf.interval__start) as b
	on b.interval__start = a.interval__start
	order by interval__start
);

create view view__monitorlf_unitname as (
    select unitname, min(interval__start) as interval__start from monitorlf m
    group by (ids__uid,unitname)
    order by interval__start
);

create view view__monitorhf_unitname as (
    select unitname, min(interval__start) as interval__start from monitorhf m
    group by (ids__uid,unitname)
    order by interval__start
);
