SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

select p.Property_key, p.TPKLPAPROP, p.Formatted_Address, p.Suburb
    ,ps.Service_Code, sum(ps.Number_of_Services) as number_of_services
from infodbo.Property p
    inner join infodbo.Property_Services_Facts ps on ps.Property_key = p.Property_key
where p.Status = 'H'
    and  ps.Status = 'C'
    and (ps.Removal_Date is null or ps.Removal_Date > getdate())
group by p.Property_key, p.TPKLPAPROP, p.Formatted_Address, p.Suburb, ps.Service_Code
order by TPKLPAPROP