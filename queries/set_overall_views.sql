


create view view__has as
(
	select ids__uid, ids__interval,
		case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_btb v)) then 1 else 0 end as "btb",
		case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_rf v)) then 1 else 0 end as "rf",
		case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_spo2 v)) then 1 else 0 end as "spo2",
        case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_arts v)) then 1 else 0 end as "arts",
        case when (via.ids__interval in (select v.ids__interval from view__monitorlf_has_allsignals v)) then 1 else 0 end as "allsignals",
		case when (via.ids__interval in (select v.ids__interval from view__vikt_has v)) then 1 else 0 end as "vikt",
        case when (via.ids__interval in (select v.ids__interval from view__med_has_caffein v)) then 1 else 0 end as "caffein",

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
        case when (vua.ids__uid in (select v.ids__uid from view__monitorlf_uid_has_onesignal v)) then 1 else 0 end as "onesignal",
        case when (vua.ids__uid in (select v.ids__uid from view__monitorlf_uid_has_allsignals v)) then 1 else 0 end as "allsignals",
		case when (vua.ids__uid in (select v.ids__uid from view__vikt_uid_has v)) then 1 else 0 end as "vikt",
		case when (vua.ids__uid in (select v.ids__uid from view__overview_uid_has v)) then 1 else 0 end as "overview",
		case when (vua.ids__uid in (select v.ids__uid from view__takecare_uid_has v)) then 1 else 0 end as "takecare",
        case when (vua.ids__uid in (select v.ids__uid from view__clinisoft_uid_has v)) then 1 else 0 end as "clinisoft",
        case when (vua.ids__uid in (select v.ids__uid from view__monitorlf_uid_has v)) then 1 else 0 end as "monitorlf",
        case when (vua.ids__uid in (select v.ids__uid from view__monitorhf_uid_has v)) then 1 else 0 end as "monitorhf",
        case when (vua.ids__uid in (select v.ids__uid from view__overview_uid_neo v)) then 1 else 0 end as "neo",
        case when (vua.ids__uid in (select v.ids__uid from view__overview_uid_vlbw v)) then 1 else 0 end as "vlbw",
        case when (vua.ids__uid in (select v.ids__uid from view__overview_uid_preterm v)) then 1 else 0 end as "preterm",
        case when (vua.ids__uid in (select v.ids__uid from view__overview_uid_term v)) then 1 else 0 end as "term",
        case when (vua.ids__uid in (select v.ids__uid from view__overview_uid_extremely_preterm v)) then 1 else 0 end as "extemely_preterm",
        case when (vua.ids__uid in (select v.ids__uid from view__overview_uid_very_preterm v)) then 1 else 0 end as "very_preterm",
        case when (vua.ids__uid in (select v.ids__uid from view__overview_uid_late_preterm v)) then 1 else 0 end as "late_preterm",
        case when (vua.ids__uid in (select v.ids__uid from view__med_uid_has_caffein v)) then 1 else 0 end as "caffein",
        case when (vua.ids__uid in (select v.ids__uid from view__caffein_uid_has_validloadingdose v)) then 1 else 0 end as "caffein_validloadingdose",

        $REGISTERED_UID_TK_EVENTS$
	from view__uid_all vua
);
