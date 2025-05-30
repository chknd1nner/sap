SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

select *
,case 
	when (isnull(service_bin_type,0) != isnull(rate_bin_type,0)) then 'Yes'
	else 'No'
end as bin_type_mismatch
,case 
	when (isnull(avg_frequency,0) != isnull(rate_frequency,0)) then 'Yes'
	else 'No'
end as frequency_mismatch
,case 
	when substring(Ratetype, 3, 1) != 'M' then 'Yes'
	else 'No'
end as rate_type_mismatch
,isnull(child_formatted_address, parent_formatted_address) as formatted_address
,isnull(child_suburb, parent_suburb) as suburb
from 
(
	select *
		,(total_volume / eligible_properties) avg_bin_size
		,(select upper(C_Code) from infodbo.Reg_BBAND_Bin_Bands where C_MINIMUM_VALUE <= (total_volume / eligible_properties) and C_MAXIMUM_VALUE >= (total_volume / eligible_properties) and left(C_CALCULATION_METHOD, 3) = 'SHA') service_bin_type
	from 
	(
		select p.Property_key as parent_key, p.TPKLPAPROP as parent_TPKLPAPROP, p.Formatted_Address as parent_formatted_address, p.Suburb as parent_suburb
      ,sum(try_convert(int, ps.Container_Type) * ps.Number_of_Collections * ps.Number_of_Services) as total_volume
      ,avg(cast(ps.Number_of_Collections as decimal)) avg_frequency
		from infodbo.Property p
			inner join infodbo.Property_Services_Facts ps on ps.Property_key = p.Property_key
			inner join infodbo.Reg_BSERV_Bin_Services pbr on pbr.Property_Key_LINK_TO_PROPERTY = p.Property_key
		where p.Status = 'C' 
			and (ps.Status = 'C' or ps.Removal_Date > getdate())
			and left(ps.Service_code, 1) = 'R'
			and left(pbr.C_CALCULATION_METHOD, 3) = 'SHA'
		group by p.Property_key, p.TPKLPAPROP, p.Formatted_Address, p.Suburb
	) parent_properties
  left join
  (
    select C_AUDIT_DATE, Property_Key_LINK_TO_PROPERTY from infodbo.Reg_BSERV_Bin_Services
  ) pbr
  on pbr.Property_Key_LINK_TO_PROPERTY = parent_properties.parent_key
	left join 
	(
		select cp.Parent_Property_Key, cp.Property_key, cp.Parent_Property_TPKLPAPROP, cp.TPKLPAPROP
			,count(cp.Property_key) over (partition by cp.Parent_Property_Key) as eligible_properties, cp.Formatted_Address as child_formatted_address, cp.Suburb as child_suburb
			,rtf.Rate_Type_Key, rtf.Units as rate_units
			,rt.Ratetype_Key, rt.Ratetype, upper(substring(rt.ratetype,4,3)) rate_bin_type, try_convert(int, right(rtrim(rt.Ratetype),2)) rate_frequency
			,ra.Assessment, ra.Vgnumber
		from infodbo.Property cp
			inner join infodbo.Rate_Type_Facts rtf on rtf.Property_key = cp.Property_key
			inner join infodbo.Rate_Type rt on rt.Ratetype_key = rtf.Ratetype_Key
			inner join infodbo.Rate_Assessment ra on ra.Assessment_Key = rtf.Assessment_Key
		where rt.IS_Current_Period = 'Y'
			and (rtf.Suspended_Date is null or rtf.Suspended_Date > getdate())
			and rt.Category_Code = 'DWM'
	) rates
	on parent_properties.parent_key = rates.Parent_Property_Key
) muds
where not ((isnull(service_bin_type,0) != isnull(rate_bin_type,0)
		or isnull(avg_frequency,0) != isnull(rate_frequency,0)
		or substring(Ratetype, 3, 1) != 'M'))
order by parent_TPKLPAPROP, TPKLPAPROP