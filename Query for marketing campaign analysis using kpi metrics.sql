/*In the utm_parameters field, Cyrillic letters were encoded in the URL string. 
Decoded the utm_campaign value by creating a temporary function. 
*/

CREATE OR REPLACE FUNCTION pg_temp.decode_url_part(p varchar) RETURNS varchar AS $$
select
  convert_from(CAST(E'\\x' || string_agg(CASE WHEN length(r.m[1]) = 1 THEN encode(convert_to(r.m[1], 'SQL_ASCII'), 'hex') ELSE substring(r.m[1] from 2 for 2) END, '') AS bytea), 'UTF8')
FROM regexp_matches($1, '%[0-9a-f][0-9a-f]|.', 'gi') AS r(m);
$$ LANGUAGE SQL IMMUTABLE STRICT;

/* Combining Data in the CTE Query
combined data from the specified tables in a CTE query to obtain:
-ad_date as the date of ad display on Google and Facebook.
-url_parameters as the part of the URL from the campaign link, which includes UTM parameters.
-The metrics (spend, impressions, reach, clicks, leads, value) for campaigns and ad sets on the relevant days, setting any null values to zer
*/

with common_table as (select ad_date, 
	url_parameters, 
	campaign_name,
	coalesce (spend,0) as spend,
	coalesce (impressions,0) as impressions, 
	coalesce (reach,0) as reach, 
	coalesce (clicks,0) as clicks,
	coalesce (leads,0) as leads,
	coalesce (value,0) as value
from facebook_ads_basic_daily fabd
join  facebook_adset fa on fabd.adset_id = fa.adset_id 
join facebook_campaign fc on fabd.campaign_id = fc.campaign_id
union all
select ad_date, 	
	url_parameters, 
	campaign_name,
	coalesce (spend,0) as spend,
	coalesce (impressions,0) as impressions, 
	coalesce (reach,0) as reach, 
	coalesce (clicks,0) as clicks,
	coalesce (leads,0) as leads, 
	coalesce (value,0) as value
from google_ads_basic_daily gabd),

/* Creating a Sample from the CTE
 From the resulting CTE, created a sample with:
- extracted month as the month of advertisement display.
-utm_campaign extracted from the utm_parameters field.
-Used the substring function with a regular expression to handle the extraction.
-Calculated the total amount of expenses, number of impressions, number of clicks, and total conversion value for each date and campaign.
-Computed CTR(click through rate), CPC(cost per click), CPM(cost per mile), 
and ROMI(return of marketing investment) for each date and campaign, 
using the CASE statement to avoid division by zero errors.
 */
common_tab2 as (
	select date(date_trunc ('month', ad_date)) as ad_month,
	case when lower(substring(url_parameters, 'utm_campaign=([^&#$]+)')) = 'nan' then null 
	else pg_temp.decode_url_part((lower(substring(url_parameters, 'utm_campaign=([^&#$]+)')))) end as utm_campaign,
	sum(spend) as total_spend,
	sum(impressions) as total_impressions,
	sum(clicks) as total_clicks,
	sum(value) as total_value,
	case when Sum (impressions) > 0 then round(Sum (clicks)*100/ Sum (impressions):: numeric, 2) else '0' end as CTR,
	case when Sum (clicks) > 0 then round (Sum (spend):: numeric  /Sum (clicks),2)  else '0' end as CPC,
	case when Sum (impressions) > 0  then round((sum (spend):: numeric  / Sum (impressions) )* 1000 ) else '0' end as CPM,
	case when Sum (spend) > 0 then ROUND((SUM(value) - SUM(spend))*100 / SUM(spend)::numeric, 2) else '0' end as ROMI
from common_table 
group by 1,2)

/*
 For each utm_campaign in each month, added new fields to calculate the percentage difference in CPC, CTR, and ROMI compared to the previous month.
 */
SELECT 
  	c1.ad_month,
 	c1.utm_campaign,
  	c1.total_spend,
	c1.total_impressions,
	c1.total_clicks,
	c1.total_value,
	c1.CTR,
	c1.CPM,
  	CASE  WHEN c1.CTR != 0 THEN round(((c1.CTR - c2.CTR)*100/ c2.CTR),2) END AS ctr_diff,
  	c1.cpc,
	CASE  WHEN c1.cpc != 0 THEN  round((c1.Cpc - c2.Cpc)*100/ c2.Cpc,2) END AS cpc_diff,
  	c1.romi,
	CASE  WHEN c1.romi != 0 THEN  round((c1.romi - c2.romi)*100/ c2.romi,2) END AS romi_diff
FROM 
  common_tab2 c1
LEFT JOIN 
  common_tab2 c2 ON c1.ad_month = c2.ad_month + INTERVAL '1 month' AND c1.utm_campaign = c2.utm_campaign
order by  c1.utm_campaign, c1.ad_month
/*
  Another option of query using window fx to add new fields to calculate the percentage difference in CPC, CTR, and ROMI compared to the previous month.
 */

select ad_month,
	utm_campaign,
	total_spend,
	total_impressions,
	total_clicks,
	total_value,
	CTR,
	round(((CTR - lag (CTR, 1) over (partition by utm_campaign order by ad_month))) *100/ 
	lag (CTR, 1) over (partition by utm_campaign order by ad_month),2) as ctr_diff_per,
	CPC,
	CPM,
	round(((CPM - lag (CPM, 1) over (partition by utm_campaign order by ad_month)) *100) /
	lag (CPM, 1) over (partition by utm_campaign order by ad_month),2) as cpm_diff_prev,
	ROMI,
	round(((ROMI -lag (ROMI, 1) over (partition by utm_campaign order by ad_month))*100)/ 
	lag (ROMI, 1) over (partition by utm_campaign order by ad_month),2) as romi_diff_prev
from common_tab2 



