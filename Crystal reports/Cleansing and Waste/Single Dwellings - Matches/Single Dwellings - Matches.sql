SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

select *
from
(
	select *	
    ,case 
      when (isnull(service_bin_type,0) != isnull(rate_bin_type,0)) then 'Yes'
      else 'No'
    end as bin_type_mismatch
    ,case 
      when (isnull(service_frequency,0) != isnull(rate_frequency,0)) then 'Yes'
      else 'No'
    end as frequency_mismatch
    ,case 
      when (isnull(service_units,0) != isnull(rate_units,0)) then 'Yes'
      else 'No'
    end as units_mismatch
    ,case 
      when substring(Ratetype, 3, 1) != 'S' then 'Yes'
      else 'No'
    end as rate_type_mismatch
    ,sum(case when service_bin_type = rate_bin_type then 1 else 0 end) over (partition by TPKLPAPROP) as number_bin_matched
    ,sum(case when service_bin_type != rate_bin_type then 1 else 0 end) over (partition by TPKLPAPROP) as number_bin_mismatched
	from
	(
		select p.Property_key, p.TPKLPAPROP, p.Formatted_Address, p.Suburb, p.Property_Type
			,psg.service_bin_type, psg.service_frequency, psg.service_units, psg.avg_bin_size
			,left(psg.C_CALCULATION_METHOD, 3) as calculation_method
			,count(service_bin_type) over (partition by p.TPKLPAPROP) as number_service_records
			,pbr.C_AUDIT_DATE
		from infodbo.Property p
			inner join (
				select Property_key, tpklpaprop, bin_type as service_bin_type, C_CALCULATION_METHOD, Number_of_Collections as service_frequency, sum(Number_of_Services) as service_units, avg(bin_size) as avg_bin_size
				from
				(
					select *
					,try_convert(int,Container_Type) bin_size
					,(select upper(C_Code) from infodbo.Reg_BBAND_Bin_Bands where C_MINIMUM_VALUE <= try_convert(int,Container_Type) and C_MAXIMUM_VALUE >= try_convert(int,Container_Type) and C_CALCULATION_METHOD = pbr.C_CALCULATION_METHOD) bin_type
					from infodbo.Property_Services_Facts ps
						inner join infodbo.Reg_BSERV_Bin_Services pbr on pbr.Property_Key_LINK_TO_PROPERTY = ps.Property_key
					where (Status = 'C' or Removal_Date > getdate())
						and left(Service_Code, 1) = 'R'
						and left(pbr.C_CALCULATION_METHOD, 3) = 'SNG'
				) ps
				group by Property_key, tpklpaprop, bin_type, C_CALCULATION_METHOD, Number_of_Collections
			) psg on psg.Property_key = p.Property_key
			left join infodbo.Reg_BSERV_Bin_Services pbr on pbr.Property_Key_LINK_TO_PROPERTY = p.Property_key
		where p.Status = 'C'
	) property_services
	left join
	(
		select rtf.Property_key as rate_property_key, rtf.Rate_Type_Key, rtf.Units as rate_units
			,rt.Ratetype_Key, rt.Ratetype, upper(substring(rt.ratetype,4,3)) rate_bin_type, try_convert(int, right(rtrim(rt.Ratetype),2)) rate_frequency
			,ra.Assessment, ra.Vgnumber
			,count(rt.Ratetype) over (partition by rtf.Property_key) as number_rate_records
		from infodbo.Rate_Type_Facts rtf
			inner join infodbo.Rate_Type rt on rt.Ratetype_key = rtf.Ratetype_Key
			inner join infodbo.Rate_Assessment ra on ra.Assessment_Key = rtf.Assessment_Key
		where rt.IS_Current_Period = 'Y'
			and (rtf.Suspended_Date is null or rtf.Suspended_Date > getdate())
			and rt.Category_Code = 'DWM'		
	) rates
	on property_services.Property_key = rates.rate_property_key and (property_services.service_bin_type = rates.rate_bin_type or 1 = 1)
) services_rates
where not (bin_type_mismatch = 'Yes'
			or frequency_mismatch = 'Yes'
			or units_mismatch = 'Yes'
			or rate_type_mismatch = 'Yes')
order by services_rates.TPKLPAPROP