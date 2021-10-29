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


create view view__monitorlf_has_btb as (
select ids__uid ,ids__interval
from monitorlf ddwl
where ddwl."lf__147840__147850__147850__btbhf__btbhf__spm" notnull or ddwl."lf__none__147850__none__btbhf__none__spm" notnull
);


create view view__monitorlf_has_rf as (
select ids__uid, ids__interval
from monitorlf ddwl
where
ddwl."lf__151562__151562__151562__rf__rf__rpm" notnull or ddwl."lf__none__151562__none__rf__none__rpm" notnull
					or ddwl."lf__151562__151562__151562__rf__rf__ok-nd" notnull or ddwl."lf__none__151562__none__rf__none__okand" notnull
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


create view view__vikt_has as
(
	select ids__uid ,ids__interval
	from vikt
	where cirk_vikt notnull
);


create view view__has as
(
	select ids__uid, ids__interval,
		case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_btb v)) then 1 else 0 end as "btb",
		case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_rf v)) then 1 else 0 end as "rf",
		case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_spo2 v)) then 1 else 0 end as "spo2",
		case when (via.ids__interval in (select v.ids__interval from view__vikt_has v)) then 1 else 0 end as "vikt",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_abdominal_nec v)) then 1 else 0 end as "abdominal_nec",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_brain_ivh_stage_3_4 v)) then 1 else 0 end as "brain_ivh_stage_3_4",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_cns v)) then 1 else 0 end as "cns",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_cns_infection v)) then 1 else 0 end as "cns_infection",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_death v)) then 1 else 0 end as "death",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_eos v)) then 1 else 0 end as "eos",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_infection v)) then 1 else 0 end as "infection",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_los v)) then 1 else 0 end as "los",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_lung_bleeding v)) then 1 else 0 end as "lung_bleeding",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_pneumonia v)) then 1 else 0 end as "pneumonia",
		case when (via.ids__interval in (select v.ids__interval from view__tkgrp_sro v)) then 1 else 0 end as "sro"
	from view__interv_all via
);
