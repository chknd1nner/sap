SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

select distinct ps.parent_TPKLPAPROP, ps.TPKLPAPROP, ps.Formatted_Address, ps.Suburb, r.Assessment, r.Vgnumber,
                r.Ratetype, r.RatePayer, bs.C_AUDIT_DATE as Audit_Date, ps.service_category, has_N0000UA
from
(
	select *
		,sum(number_of_services) over (partition by TPKLPAPROP) as total_number_of_services
		,case when sum(number_of_N0000UA) over (partition by TPKLPAPROP) > 0 then 'Yes' else 'No' end as has_N0000UA
	from
	(
		select isnull(cp.Property_key, p.Property_key) as Property_key, isnull(cp.TPKLPAPROP, p.TPKLPAPROP) as TPKLPAPROP, p.TPKLPAPROP as parent_TPKLPAPROP, isnull(cp.Formatted_Address, p.Formatted_Address) as Formatted_Address, isnull(cp.Suburb, p.Suburb) as Suburb
			,isnull(sum(case when (ps.Status = 'C' or ps.Removal_Date > getdate()) and ps.Service_Code != 'N0000UA' then ps.Number_of_Services else 0 end), 0) + isnull(sum(case when (cps.Status = 'C' or cps.Removal_Date > getdate()) and cps.Service_Code != 'N0000UA' then cps.Number_of_Services else 0 end), 0) as number_of_services
			,isnull(sum(case when ps.Service_Code like 'R%' and (ps.Status = 'C' or ps.Removal_Date > getdate()) then ps.Number_of_Services else 0 end), 0) + isnull(sum(case when cps.Service_Code like 'R%' and (cps.Status = 'C' or cps.Removal_Date > getdate()) then cps.Number_of_Services else 0 end), 0) as number_of_red_services
			,case
				when isnull(sum(case when cps.Status = 'C' or cps.Removal_Date > getdate() then cps.Number_of_Services else 0 end), 0) > 0 then 'Child Has Service'
				when sc.Is_Active is null then 'Missing Service'
				when isnull(sum(case when ps.Status = 'C' or ps.Removal_Date > getdate() then ps.Number_of_Services else 0 end), 0) = 0 then 'Zero Service'
				when sc.Is_Active = 'Yes' then 'Has Service'
				when sc.Is_Active = 'No' then 'Old Service Code'
				else 'Unknown'
			end as service_category
			,sum(case when (ps.Service_Code = 'N0000UA' and sc.Is_Active = 'Yes') or (cps.Service_Code = 'N0000UA' and csc.Is_Active = 'Yes')  then 1 else 0 end) as number_of_N0000UA
			,case when csc.Is_Active is not null and sc.Is_Active is null then 'Yes' else 'No' end as is_terrace_title
		from infodbo.Property p
			left join infodbo.Property cp on cp.Parent_Property_Key = p.Property_key
			left join infodbo.Property_Services_Facts cps on cps.Property_key = cp.Property_key
			left join infodbo.Property_Service_Codes_Custom csc on csc.Service_Code = cps.Service_Code
			left join infodbo.Property_Services_Facts ps on ps.Property_key = p.Property_key
			left join infodbo.Property_Service_Codes_Custom sc on sc.Service_Code = ps.Service_Code
		where p.Parent_Property_Key is null
			and p.Status = 'C'
			and (cp.Status is null or cp.Status = 'C')
			and (ps.Property_key is null or (ps.Service_Code like 'R%' or (ps.Status = 'C' and ps.Service_Code like 'G%' and sc.Is_Active = 'No') or ps.Service_Code like 'N%'))
		group by p.Property_key, cp.Property_key, p.TPKLPAPROP, cp.TPKLPAPROP, cp.Parent_Property_TPKLPAPROP, p.Formatted_Address, cp.Formatted_Address, p.Suburb, cp.Suburb, sc.Is_Active, csc.Is_Active

		--Added below part for SRV-425049 - begin
	    UNION ALL

	    select p.Property_key as Property_key, p.TPKLPAPROP as TPKLPAPROP, ISNULL(p.Parent_Property_Key, p.TPKLPAPROP) as parent_TPKLPAPROP, p.Formatted_Address as Formatted_Address, p.Suburb as Suburb
			,isnull(sum(case when (ps.Status = 'C' or ps.Removal_Date > getdate()) and ps.Service_Code != 'N0000UA' then ps.Number_of_Services else 0 end), 0) as number_of_services
			,isnull(sum(case when ps.Service_Code like 'R%' and (ps.Status = 'C' or ps.Removal_Date > getdate()) then ps.Number_of_Services else 0 end), 0) as number_of_red_services
			,case
				when sc.Is_Active is null then 'Missing Service'
				when isnull(sum(case when ps.Status = 'C' or ps.Removal_Date > getdate() then ps.Number_of_Services else 0 end), 0) = 0 then 'Zero Service'
				when sc.Is_Active = 'Yes' then 'Has Service'
				when sc.Is_Active = 'No' then 'Old Service Code'
				else 'Unknown'
			end as service_category
			,sum(case when (ps.Service_Code = 'N0000UA' and sc.Is_Active = 'Yes') then 1 else 0 end) as number_of_N0000UA
			,'No' as is_terrace_title
		from infodbo.Property p
			left join infodbo.Property_Services_Facts ps on ps.Property_key = p.Property_key
			left join infodbo.Property_Service_Codes_Custom sc on sc.Service_Code = ps.Service_Code
		where p.Parent_Property_Key is null
			and p.Status = 'C'
			and (ps.Property_key is null or (ps.Service_Code like 'R%' or (ps.Status = 'C' and ps.Service_Code like 'G%' and sc.Is_Active = 'No') or ps.Service_Code like 'N%'))
		group by p.Property_key, p.TPKLPAPROP, p.Parent_Property_Key, p.Formatted_Address, p.Suburb, sc.Is_Active
	    --Added below part for SRV-425049 - end
	) ps
) ps
inner join
(
	select rtf.Property_key, rtf.Rate_Type_Key, rtf.Units as rate_units
		,rt.Ratetype_Key, rt.Ratetype, upper(substring(rt.ratetype,4,3)) rate_bin_type, try_convert(int, right(rtrim(rt.Ratetype),2)) rate_frequency
		,ra.Assessment, ra.Vgnumber, ra.Ratepayer_Address_1 as RatePayer
	from infodbo.Rate_Type_Facts rtf
		inner join infodbo.Rate_Type rt on rt.Ratetype_key = rtf.Ratetype_Key
		inner join infodbo.Rate_Assessment ra on ra.Assessment_Key = rtf.Assessment_Key
	where rt.IS_Current_Period = 'Y'
		and (rtf.Suspended_Date is null or rtf.Suspended_Date > getdate())
		and rt.Category_Code = 'DWM'
) r
on ps.Property_key = r.Property_key
left join infodbo.Reg_BSERV_Bin_Services bs on ps.property_key = bs.property_key_link_to_property
where Ratetype != 'N0000UA'
	and (total_number_of_services = 0 or service_category = 'Old Service Code')
	and is_terrace_title = 'No'


UNION

				
select  distinct p.Parent_Property_TPKLPAPROP AS parent_TPKLPAPROP, p.TPKLPAPROP, p.Formatted_Address, p.Suburb, r.Assessment, r.Vgnumber,
                 r.Ratetype, r.RatePayer, bs.C_AUDIT_DATE as Audit_Date, 'Child Has Service' as service_category, 'Yes'  as has_N0000UA
--select  distinct p.TPKLPAPROP, p.Property_key, p.Parent_Property_TPKLPAPROP, p.Parent_Property_Key,				
--ps.Service_Code AS 'Child Service Code', bs.C_CALCULATION_METHOD AS 'Child Calc Method', 				
--parent_ps.Service_Code AS 'Parent Service Code', parent_bs.C_CALCULATION_METHOD AS 'Parent Calc Method', 				
--p.Formatted_Address, r.RateType 'Child Rate Type', parent_r.Ratetype 'Parent Rate Type', ps.Number_of_Services				
from infodbo.Property p 				
				left join infodbo.Property_Services_Facts ps on ps.Property_key = p.Property_key
				left join infodbo.Property_Services_Facts parent_ps on parent_ps.Property_key = p.Parent_Property_Key
				left join infodbo.Reg_BSERV_Bin_Services bs on bs.Property_Key_LINK_TO_PROPERTY = p.Property_key
				left join infodbo.Reg_BSERV_Bin_Services parent_bs on parent_bs.Property_Key_LINK_TO_PROPERTY = p.Parent_Property_Key
				
inner join				
(				
	select rtf.Property_key, rtf.Rate_Type_Key, rtf.Units as rate_units			
		,rt.Ratetype_Key, rt.Ratetype, upper(substring(rt.ratetype,4,3)) rate_bin_type, try_convert(int, right(rtrim(rt.Ratetype),2)) rate_frequency		
		,ra.Assessment, ra.Vgnumber, ra.Ratepayer_Address_1 as RatePayer		
	from infodbo.Rate_Type_Facts rtf			
		inner join infodbo.Rate_Type rt on rt.Ratetype_key = rtf.Ratetype_Key		
		inner join infodbo.Rate_Assessment ra on ra.Assessment_Key = rtf.Assessment_Key		
	where rt.IS_Current_Period = 'Y'			
		and (rtf.Suspended_Date is null or rtf.Suspended_Date > getdate())		
		and rt.Category_Code = 'DWM'		
) r				
on ps.Property_key = r.Property_key				
left join				
(				
	select rtf.Property_key, rtf.Rate_Type_Key, rtf.Units as rate_units			
		,rt.Ratetype_Key, rt.Ratetype, upper(substring(rt.ratetype,4,3)) rate_bin_type, try_convert(int, right(rtrim(rt.Ratetype),2)) rate_frequency		
		,ra.Assessment, ra.Vgnumber, ra.Ratepayer_Address_1 as RatePayer		
	from infodbo.Rate_Type_Facts rtf			
		inner join infodbo.Rate_Type rt on rt.Ratetype_key = rtf.Ratetype_Key		
		inner join infodbo.Rate_Assessment ra on ra.Assessment_Key = rtf.Assessment_Key		
	where rt.IS_Current_Period = 'Y'			
		and (rtf.Suspended_Date is null or rtf.Suspended_Date > getdate())		
		and rt.Category_Code = 'DWM'		
) parent_r				
on parent_ps.Property_key = parent_r.Property_key				
				
where 				
p.Status = 'C'				
AND ps.Status = 'C'				
-- SRV-463575 change to include parent who do not have any current services - not just the ones that do not have any services at all			
AND (parent_ps.Service_Code IS NULL				
           OR NOT EXISTS (SELECT psf.Service_Code FROM infodbo.Property_Services_Facts psf WHERE (psf.Property_key = parent_ps.Property_key AND 
                    psf.Status = 'C') ))					
AND parent_bs.C_CALCULATION_METHOD IS NULL				
AND parent_r.RateType IS NULL				
AND Parent_Property_Key IS NOT NULL		
AND ps.Service_Code = 'N0000UA' 
AND bs.C_CALCULATION_METHOD like 'MAN%'


order by ps.TPKLPAPROP
