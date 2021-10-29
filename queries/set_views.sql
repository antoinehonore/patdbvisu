create view view__interv_all as (
	select ids__uid,ids__interval from monitorlf
	union
	select ids__uid,ids__interval from vikt
	union
	select ids__uid,ids__interval from vatska
	union
	select ids__uid,ids__interval from takecare
	union
	select ids__uid,ids__interval from respirator
	union
	select ids__uid,ids__interval from pressure
	union
	select ids__uid,ids__interval from med
	union
	select ids__uid,ids__interval from lab
	union
	select ids__uid,ids__interval from fio2
);

create view view__uid_all as (
    select distinct ids__uid
    from view__interv_all
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
where
ddwl."lf__151562__151562__151562__rf__rf__rpm" notnull or ddwl."lf__none__151562__none__rf__none__rpm" notnull
					or ddwl."lf__151562__151562__151562__rf__rf__ok-nd" notnull or ddwl."lf__none__151562__none__rf__none__okand" notnull
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


create view view__overview_uid_has as
(
	select distinct ids__uid
	from overview
);

create view view__takecare_uid_has as
(
	select distinct ids__uid
	from takecare
	where extra notnull
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



create view view__has as
(
	select ids__uid, ids__interval,
		case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_btb v)) then 1 else 0 end as "btb",
		case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_rf v)) then 1 else 0 end as "rf",
		case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_spo2 v)) then 1 else 0 end as "spo2",
        case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_arts v)) then 1 else 0 end as "arts",
		case when (via.ids__interval in (select v.ids__interval from view__vikt_has v)) then 1 else 0 end as "vikt",
        $REGISTERED_TK_EVENTS$
	from view__interv_all via
);


create view view__uid_has as
(
	select ids__uid,
		case when (vua.ids__uid in (select v.ids__uid from view__monitorlf_uid_has_btb v)) then 1 else 0 end as "btb",
		case when (vua.ids__uid in (select v.ids__uid from view__monitorlf_uid_has_rf v)) then 1 else 0 end as "rf",
		case when (vua.ids__uid in (select v.ids__uid from view__monitorlf_uid_has_spo2 v)) then 1 else 0 end as "spo2",
        case when (vua.ids__uid in (select v.ids__uid from view__monitorlf_uid_has_arts v)) then 1 else 0 end as "arts",
		case when (vua.ids__uid in (select v.ids__uid from view__vikt_uid_has v)) then 1 else 0 end as "vikt",
		case when (vua.ids__uid in (select v.ids__uid from view__overview_uid_has v)) then 1 else 0 end as "overview",
		case when (vua.ids__uid in (select v.ids__uid from view__takecare_uid_has v)) then 1 else 0 end as "takecare",
        $REGISTERED_UID_TK_EVENTS$
	from view__uid_all vua
);


