SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

select *
from
(
	select *
		,isnull(sum(rate_units) over (partition by parent_Property_key), 0) as total_child_rate_units
		,isnull(sum(number_of_rates) over (partition by parent_Property_key), 0) as total_child_rates
	from
	(
		select isnull(cp.Property_key, p.Property_key) as Property_key, isnull(cp.TPKLPAPROP, p.TPKLPAPROP) as TPKLPAPROP, isnull(cp.Formatted_Address, p.Formatted_Address) as Formatted_Address, isnull(cp.Suburb, p.Suburb) as Suburb
			,p.Property_key as parent_Property_key, p.TPKLPAPROP as parent_TPKLPAPROP, p.Formatted_Address as parent_Formatted_Address, p.Suburb as parent_Suburb
			,cp.Property_key as child_Property_key, cp.TPKLPAPROP as child_TPKLPAPROP, cp.Formatted_Address as child_Formatted_Address, cp.Suburb as child_Suburb
			,count(cp.Property_key) over (partition by p.Property_key) as number_of_all_child_properties
			,sum(case when (cp.Formatted_Address not like 'Car Space%' and cp.Formatted_Address not like 'Storage%') then 1 else 0 end) over (partition by p.Property_key) as number_of_child_properties
			,isnull(rates.number_of_rates, 0) as parent_number_of_rates, assessment.Assessment as parent_Assessment, assessment.Vgnumber as parent_Vgnumber
			,isnull(sum(ps.Number_of_Services), 0) as number_of_services, sum(case when left(ps.Service_Code, 1) = 'R' then ps.Number_of_Services else 0 end) as number_of_red_services
			,case
				when sc.Is_Active is null then 'Missing Service'
				when isnull(sum(ps.Number_of_Services), 0) = 0 then 'Zero Service'
				when sc.Is_Active = 'Yes' then 'Has Service'
				when sc.Is_Active = 'No' then 'Old Service Code'
				else 'Unknown'
			end as service_category
			,isnull(left(bs.C_CALCULATION_METHOD, 4), 'None') as calculation_method
		from infodbo.Property p
			left join infodbo.Property cp on cp.Parent_Property_Key = p.Property_key
			left join infodbo.Property_Services_Facts ps on ps.Property_key = p.Property_key
			left join infodbo.Property_Service_Codes_Custom sc on sc.Service_Code = ps.Service_Code
			left join infodbo.Reg_BSERV_Bin_Services bs on bs.Property_Key_LINK_TO_PROPERTY = p.Property_key
			left join 
			(
				select TFKLPAPROP, Assessment, Vgnumber
				from infodbo.Rate_Assessment 
			) assessment on assessment.TFKLPAPROP = p.TPKLPAPROP
			left join
			(
				select rtf.Property_key, rtf.Rate_Type_Key, rtf.Units as rate_units, isnull(count(rtf.Property_key) over (partition by rtf.Property_key), 0) as number_of_rates
					,rt.Ratetype_Key, rt.Ratetype, upper(substring(rt.ratetype,4,3)) rate_bin_type, try_convert(int, right(rtrim(rt.Ratetype),2)) rate_frequency
				from infodbo.Rate_Type_Facts rtf
					inner join infodbo.Rate_Type rt on rt.Ratetype_key = rtf.Ratetype_Key
				where rt.IS_Current_Period = 'Y'
					and (rtf.Suspended_Date is null or rtf.Suspended_Date > getdate())
					and rt.Category_Code = 'DWM'		
			) rates on rates.Property_key = p.Property_key
		where p.Parent_Property_Key is null
			and p.Status = 'C'
			and (cp.Status is null or cp.Status = 'C')
			and (ps.Property_key is null or ((ps.Status = 'C' or ps.Removal_Date > getdate()) and (ps.Service_Code like 'R%' or (ps.Service_Code like 'G%' and sc.Is_Active like 'No%') or ps.Service_Code like 'N%')))
		group by p.Property_key, cp.Property_key, p.TPKLPAPROP, cp.TPKLPAPROP, p.Formatted_Address, cp.Formatted_Address, p.Suburb, cp.Suburb, sc.Is_Active, bs.C_CALCULATION_METHOD, rates.number_of_rates, assessment.Assessment, assessment.Vgnumber
	) property_services
	left join 
	(
		select TFKLPAPROP, Assessment, Vgnumber
		from infodbo.Rate_Assessment 
	) assessment on assessment.TFKLPAPROP = property_services.TPKLPAPROP
	left join
	(
		select rtf.Property_key as rates_property_key, rtf.Rate_Type_Key, rtf.Units as rate_units, isnull(count(rtf.Property_key) over (partition by rtf.Property_key), 0) as number_of_rates
			,rt.Ratetype_Key, rt.Ratetype, upper(substring(rt.ratetype,4,3)) rate_bin_type, try_convert(int, right(rtrim(rt.Ratetype),2)) rate_frequency
		from infodbo.Rate_Type_Facts rtf
			inner join infodbo.Rate_Type rt on rt.Ratetype_key = rtf.Ratetype_Key
		where rt.IS_Current_Period = 'Y'
			and (rtf.Suspended_Date is null or rtf.Suspended_Date > getdate())
			and rt.Category_Code = 'DWM'		
	) rates
	on property_services.Property_key = rates.rates_property_key
) property_services_rates
where number_of_services > 0
	and total_child_rate_units = 0
	and parent_number_of_rates = 0
	and Formatted_Address not like 'Car Space%'
	and Formatted_Address not like 'Storage%'
order by parent_TPKLPAPROP, child_TPKLPAPROP