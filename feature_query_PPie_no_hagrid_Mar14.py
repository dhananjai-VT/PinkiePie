feature_query = """
                     WITH call_data AS (
     SELECT
       v_call_logs.id AS call_id
       ,convert_timezone('America/Chicago', v_call_logs.created_at) AS call_created_at
       ,v_call_logs.call_type AS call_type
       ,case when v_call_logs.call_type=1 then 1 else 0 end as is_inbound
       ,CASE WHEN v_call_logs.call_result in (2,6) THEN 1 ELSE 0 END AS is_answered
       ,v_call_logs.call_result as call_result
       ,v_call_logs.caller_id as caller_id
       ,v_call_logs.dialed_number AS dialed_number
       ,v_call_logs.provider AS provider
       ,max(contacts.id) as contact_id
       ,max(convert_timezone('America/Chicago', contacts.created_at)) as contact_created_at
       ,max(v_call_dial_logs.talk_duration) as talk_duration
       ,max(CASE WHEN v_call_logs.call_result in (2,6) AND v_call_dial_logs.dial_result = 2 AND v_call_dial_logs.talk_duration>=90 THEN 1 ELSE 0 END) AS cc90
     FROM v_call_logs
     LEFT JOIN v_call_originator_logs ON v_call_originator_logs.v_call_log_id = v_call_logs.id
     LEFT JOIN v_call_dial_logs ON v_call_dial_logs.v_call_log_id = v_call_logs.id
     LEFT JOIN contacts ON (contacts.id=CASE WHEN v_call_logs.call_type=1 THEN v_call_originator_logs.originator_id ELSE v_call_dial_logs.dialed_id END)
     WHERE v_call_logs.call_type IN (1,2)
     AND v_call_logs.call_result != 1
     AND line_type IN ('lead','contact')
     GROUP BY 1,2,3,4,5,6,7,8,9),

dialer as 
(
select distinct v_call_logs.id AS call_id, 
max(case when (v_call_logs.call_type = 2 AND v_call_originator_logs.originator_id IN (1000,1001) AND v_call_originator_logs.originator_type = 'User') THEN 1 ELSE 2 END) AS auto_1_manual_2
from v_call_logs 
left join v_call_originator_logs ON v_call_originator_logs.v_call_log_id = v_call_logs.id
group by 1 order by 1 desc
),
        
     -- CC90s
   CC90s AS (
     SELECT *
     FROM call_data
     WHERE cc90 = 1),

previous_cc90_date as (

select call_id, contact_id, 
case when cc90=1 then call_created_at else null end as date_cc90
from call_data
),    
     
     
-- Last CC90s
   Last_CC90s AS (
     SELECT distinct call_id
     	FROM (SELECT contact_id, max(call_id) as call_id
       			FROM cc90s
       			WHERE contact_id IS NOT NULL
       			GROUP BY contact_id)),

   -- CC90_sales
   CC90s_With_Sale AS (
     SELECT distinct call_id FROM (
       SELECT clients.id, max(cc90s.call_id) as call_id
       FROM cc90s, contacts, clients
       WHERE contacts.id = cc90s.contact_id
       AND clients.id = contacts.client_id
       AND cc90s.call_created_at <= clients.created_at
       GROUP BY clients.id)),

   -- CC90 Ordinals
   CC90_Ordinals AS (
     SELECT
       call_id,
       rank() over (partition by contact_id order by call_id) as cc90_ordinal
     FROM cc90s),

   -- Attempt Ordinals
   Attempt_Ordinals AS (
     SELECT
       call_id,
       rank() over (partition by contact_id order by call_id) as attempt_ordinal
     FROM call_data),

   -- IB Attempt Ordinals
   IB_Attempt_Ordinals AS (
     SELECT
       call_id,
       rank() over (partition by contact_id order by call_id) as ib_attempt_ordinal
     FROM call_data
     WHERE call_type = 1),

   -- OB Attempt Ordinals
   OB_Attempt_Ordinals AS (
     SELECT
       call_id,
       rank() over (partition by contact_id order by call_id) as ob_attempt_ordinal
     FROM call_data
     WHERE call_type = 2),

   -- BY CONTACT DATA --

   Contact_Total_Attempts As(
     (SELECT contact_id, count(distinct call_id) as total_attempts
     FROM call_data
     GROUP BY contact_id)),

   Contact_Prior_CC90s As (
     (SELECT call_data.call_id, count(*) as prior_cc90s
      FROM call_data, cc90s
      where call_data.contact_id = cc90s.contact_id
      and cc90s.call_created_at < call_data.call_created_at
      group by call_data.call_id)),

   Contact_Total_CC90s As (
     (SELECT contact_id, count(*) as total_cc90s
      FROM cc90s
      group by contact_id)),
      
   master_calldata AS (
     SELECT
     call_data.*
     ,dlr.auto_1_manual_2
     ,CASE WHEN call_data.contact_id IS NOT NULL THEN SUM(call_data.is_answered) OVER(PARTITION BY call_data.contact_id ORDER BY call_data.call_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ELSE NULL END AS total_connects
     ,CASE WHEN call_data.contact_id IS NOT NULL THEN SUM(call_data.is_inbound) OVER(PARTITION BY call_data.contact_id ORDER BY call_data.call_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ELSE NULL END as total_inbounds
     ,CASE WHEN call_data.contact_id IS NOT NULL THEN count(call_data.call_id) OVER(PARTITION BY call_data.contact_id ORDER BY call_data.call_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ELSE NULL END AS previous_attempts
     ,CASE WHEN call_data.contact_id IS NOT NULL THEN SUM(call_data.cc90) OVER(PARTITION BY call_data.contact_id ORDER BY call_data.call_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ELSE NULL END AS previous_cc90s
     ,CASE WHEN call_data.contact_id IS NOT NULL THEN SUM(call_data.talk_duration) OVER(PARTITION BY call_data.contact_id ORDER BY call_data.call_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ELSE NULL END AS cumulative_talktime_secs
     ,lag(cc90) over (partition by call_data.contact_id order by call_data.call_id asc) as last_call_cc90
     ,last_value(previous_cc90_date.date_cc90 ignore nulls) over(partition by call_data.contact_id order by call_data.call_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_cc90_timestamp
     ,contacts.jurisdiction_id
     ,call_data.call_created_at AS date_call
     ,call_data.contact_created_at AS date_contact
     ,date_part(hrs,call_data.call_created_at) as hr_call_made
     ,date_part(d,call_data.call_created_at) as day_call_made
     ,date_part(dow,call_data.call_created_at) as dow_call_made
     ,date_part(w,call_data.call_created_at) as week_call_made
     ,date_part(hrs,call_data.contact_created_at) as hr_contact_created
     ,date_part(dow,call_data.contact_created_at) as dow_contact_created
     ,cast(DATEDIFF(hour, call_data.contact_created_at, call_data.call_created_at) as float) AS lead_age_at_call
     ,cast(DATEDIFF(hour, call_data.contact_created_at, convert_timezone('America/Chicago', getdate())) as float) AS lead_age_current
     ,contacts.valid_lead
     ,contact_statuses.name as contact_status
     ,contacts.contact_status_id
     ,CASE WHEN (call_data.call_result=2 OR call_data.call_result=6) THEN '1' WHEN call_data.call_result=3 THEN '0' WHEN (call_data.call_result=4 OR call_data.call_result=5) THEN '0' ELSE '0' END AS "Connects"
     ,lead_source.lead_source
     ,CASE WHEN lead_source.lead_source='Phone' OR lead_source.lead_source='Phone - Paid' OR lead_source.lead_source='Phone - Organic' THEN 1 ELSE 0 END AS is_Phone_Lead
     ,CASE WHEN (lead_source.lead_source='Phone' OR lead_source.lead_source='Phone - Paid' OR lead_source.lead_source='Phone - Organic') 
     	THEN NULL ELSE (
     	cast(datediff(hour, call_data.contact_created_at, first_value(call_data.call_created_at) over(partition by call_data.contact_id order by call_data.call_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as float)
     	) end as hrs_response_weblead
     ,lead_source.device
     ,CASE WHEN lead_source.lead_source IS NOT NULL THEN nvl(ew_gw_lookup.ew_gw,'GW') ELSE NULL END as ew_gw
     ,scl.subject_type
     ,scl.subject as "subject"
     ,clients.id as client_id
     ,clients.created_at as client_created_at
     ,attempt_ordinals.attempt_ordinal as attempt_ord
     ,ib_attempt_ordinals.ib_attempt_ordinal
     ,ob_attempt_ordinals.ob_attempt_ordinal
     ,cc90_ordinals.cc90_ordinal
     ,contact_prior_cc90s.prior_cc90s
     ,CASE WHEN last_cc90s.call_id IS NOT NULL THEN 1 ELSE 0 END AS last_cc90
     ,CASE WHEN clients.id IS NOT NULL THEN 1 ELSE 0 END AS client_closed_ever
     ,CASE WHEN cc90s_with_sale.call_id IS NOT NULL THEN 1 ELSE 0 END AS client_closed_this_call
     ,CASE WHEN contacts.message LIKE ('|') THEN SPLIT_PART(split_part(contacts.message,'How Soon',2),'"',1)
              ELSE SPLIT_PART(split_part(contacts.message,'How Soon: ',2),' |',1) END AS hagrid_how_soon
              
	  ,CASE WHEN contacts.message LIKE ('|') THEN SPLIT_PART(split_part(contacts.message,'Grade Level',2),'"',1)
      		ELSE SPLIT_PART(split_part(contacts.message,'Grade Level: ',2),' |',1) END AS hagrid_grade_level
      
	  ,CASE WHEN contacts.message LIKE ('|') THEN SPLIT_PART(split_part(contacts.message,'Who Needs Tutor',2),'"',1)
              ELSE SPLIT_PART(split_part(contacts.message,'Who Needs Tutor: ',2),' |',1) END AS hagrid_who
     ,contact_total_cc90s.total_cc90s
     ,contact_total_attempts.total_attempts
   FROM call_data
   left join dialer dlr ON dlr.call_id=call_data.call_id
   left join previous_cc90_date on previous_cc90_date.call_id=call_data.call_id
   LEFT JOIN contacts ON contacts.id = call_data.contact_id
   LEFT JOIN contact_statuses ON contact_statuses.id = contacts.contact_status_id
   LEFT JOIN lead_source ON lead_source.lead_id = contacts.id
   LEFT JOIN vtbid.subject_campaign_lookup scl ON scl.ad_group_id = lead_source.ad_group_id
   LEFT JOIN ew_gw_lookup ON ew_gw_lookup.identifier =
     (CASE WHEN lead_source.lead_source = 'Paid Search - Google'
                AND scl.subject_type IN ('Tutor','Tutors','Tutoring')
                THEN lead_source.lead_source || ':' || scl.subject_type || ':' || scl.subject
           WHEN lead_source.lead_source = 'Paid Search - Google'
                THEN lead_source.lead_source || ':' || scl.subject_type
           ELSE lead_source.lead_source END)
   LEFT JOIN clients ON clients.id = contacts.client_id
   LEFT JOIN cc90_ordinals ON cc90_ordinals.call_id = call_data.call_id
   LEFT JOIN attempt_ordinals ON attempt_ordinals.call_id = call_data.call_id
   LEFT JOIN ib_attempt_ordinals ON ib_attempt_ordinals.call_id = call_data.call_id
   LEFT JOIN ob_attempt_ordinals ON ob_attempt_ordinals.call_id = call_data.call_id
   LEFT JOIN last_cc90s ON last_cc90s.call_id = call_data.call_id
   LEFT JOIN cc90s_with_sale ON cc90s_with_sale.call_id = call_data.call_id
   LEFT JOIN contact_prior_cc90s ON contact_prior_cc90s.call_id = call_data.call_id
   LEFT JOIN contact_total_cc90s ON contact_total_cc90s.contact_id = call_data.contact_id
   LEFT JOIN contact_total_attempts ON contact_total_attempts.contact_id = call_data.contact_id
   LEFT JOIN taggings ON taggings.taggable_id = contacts.id AND taggings.taggable_type = 'Contact'
   LEFT JOIN tags ON tags.id = taggings.tag_id),
   
lead_cutter_data as 

(
SELECT

ltl.contact_id,
ltl.latitude,
ltl.longitude,
ltl.tags_name, 
ltl.adword_subject, 
ltl.adword_subject_type,
ltl.lead_source_roll_up,
ltl.lead_source_rolled_up_device,
ltl.created_internally,
ltl.requested_specific_tutor,
ltl.email_platform,
case when ltl.email is not null then 1 else 0 end as is_email_id_given,

ltva.first_page,
ltva.platform_name,
ltva.browser_name,
ltva.page_count,
ltva.last_page,
ltva.last_paid_page,
ltva.price_pages_post_conv

FROM sales.lead_throttling_leads ltl
LEFT JOIN sales.lead_throttling_visitors_agg ltva ON ltl.contact_id = ltva.contact_id
WHERE ltl.contact_created_date >= '2017-05-01' AND ltl.created_internally=0
ORDER BY ltl.contact_id
),

final_dataset as (SELECT

mc.call_id, 
mc.date_call as date_call,

-- call data
mc.hr_call_made,
mc.day_call_made,
mc.dow_call_made,
mc.week_call_made,
mc.is_Phone_Lead,
mc.hrs_response_weblead,
CASE WHEN mc.call_type=1 THEN 1 ELSE 0 END AS is_inbound,

--contact created data
mc.contact_id,
mc.date_contact,
mc.dow_contact_created,
mc.hr_contact_created,
 
   CASE 
   WHEN mc.subject='ACT' OR mc.subject='CFA Exam' OR mc.subject='Colleges' OR mc.subject='Competition' OR mc.subject='Competition Law' 
   OR mc.subject='Competition Math' OR mc.subject='Cpa' OR mc.subject='CPA Exam' OR mc.subject='Ged' OR mc.subject='GMAT' OR mc.subject='Gmat Verbal' 
   OR mc.subject='GRE' OR mc.subject='Gre Analytical Writing' OR mc.subject='Gre Verbal' OR mc.subject='HSPT' OR mc.subject='Hspt Language Skills' 
   OR mc.subject='Hspt Reading' OR mc.subject='Isat' OR mc.subject='ISEE' OR mc.subject='Isee Lower Level' 
   OR mc.subject='Isee Lower Level Mathematics Achievement' OR mc.subject='Isee Middle Level' OR mc.subject='Isee Primary' 
   OR mc.subject='Isee Upper Level' OR mc.subject='LSAT' OR mc.subject='Lsat Analytical Reasoning' OR mc.subject='Lsat Logical Reasoning' 
   OR mc.subject='MCAT' OR mc.subject='Mcat Biological And Biochemical Foundations Of Living Systems' 
   OR mc.subject='Mcat Chemical And Physical Foundations Of Biological Systems' 
   OR mc.subject='Mcat Psychological Social And Biological Foundations Of Behavior' OR mc.subject='Mcat Verbal Reasoning' 
   OR mc.subject='PSAT' OR mc.subject='Psat Critical Reading' OR mc.subject='Psat Mathematics' OR mc.subject='Psat Writing Skills' 
   OR mc.subject='Quantitative Reasoning' OR mc.subject='Quantitative Reasoning for Business' OR mc.subject='SAT' 
   OR mc.subject='Sat Subject Test In Biology E M' OR mc.subject='Sat Subject Test In Chemistry' OR mc.subject='Sat Subject Test In German' 
   OR mc.subject='Sat Subject Test In Japanese With Listening' OR mc.subject='Sat Subject Test In Latin' OR mc.subject='Sat Subject Test In Literature' 
   OR mc.subject='Sat Subject Test In Mathematics Level 1' OR mc.subject='Sat Subject Test In Mathematics Level 2' 
   OR mc.subject='Sat Subject Test In Physics' OR mc.subject='Sat Subject Test In Spanish' OR mc.subject='Sat Subject Test In United States History' 
   OR mc.subject='Sat Subject Test In World History' OR mc.subject='SHSAT' OR mc.subject='SSAT' OR mc.subject='Ssat Elementary Level' 
   OR mc.subject='Ssat Middle Level' OR mc.subject='Ssat Upper Level' THEN 1
   ELSE 0 END AS is_Test_Prep,
      
   CASE 
   WHEN mc.subject='' THEN 1
   ELSE 0 END AS is_Subject_NA,
   
    CASE
    WHEN mc.ew_gw='EW' THEN 1
    ELSE 0 END AS is_EW_Lead
             

,case when lcd.latitude is null then -200 else lcd.latitude end
,case when lcd.longitude is null then -200 else lcd.longitude end
,case when lcd.tags_name is null then 'None' else lcd.tags_name end
,case when lcd.lead_source_roll_up is null then 'None' else lcd.lead_source_roll_up end
,case when lcd.created_internally is null then 0 else lcd.created_internally end
,case when lcd.requested_specific_tutor is null then 0 else lcd.requested_specific_tutor end
,case when lcd.is_email_id_given is null then 0 else lcd.is_email_id_given end

 
--relationship duration data
,mc.attempt_ord
,mc.previous_attempts
,mc.lead_age_at_call as lead_age
,mc.total_connects
,mc.total_inbounds
,mc.cumulative_talktime_secs/60.0 as total_talktime_mins
 
 
 --urgency of need measurement data
,mc.last_call_cc90
,mc.last_cc90_timestamp
--,cast(DATEDIFF(hour, mc.date_call, getdate()) as float) as hrs_since_last_cc90
,mc.prior_cc90s
,mc.total_inbounds/mc.attempt_ord as percent_inbounds
,case when mc.lead_age_at_call>0 then mc.prior_cc90s/(mc.lead_age_at_call/24.0) else 0 end as frequency_cc90s
 
--to predict metrics 
,mc.talk_duration/60.0 AS talk_duration_mins
,mc.CC90
,mc.client_closed_this_call AS closed_that_call
,mc.client_closed_ever AS closed_ever

FROM master_calldata as mc
left join lead_cutter_data lcd on lcd.contact_id=mc.contact_id
WHERE mc.valid_lead=1 AND mc.contact_status_id in (1,2)
order by 1 desc)

select d.* 
from (
	select fd.*, 
	ROW_NUMBER() over (partition by fd.contact_id order by fd.attempt_ord desc) as rn
	from final_dataset fd
	) d
where d.rn=1
order by 2 desc


 """
