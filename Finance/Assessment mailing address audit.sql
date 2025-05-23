select 
  register_entry.keyval reference_no
  ,ratepayer.tpkcnaidty name_key
  ,notice_group.ntgpcode notice_group_code
  ,ratepayer.formatname ratepayer
  ,assessment.assmnumber assessment
  ,agent_address.formataddr agent_address
  ,ratepayer_override_address.formataddr override
  ,ratepayer_default_address.formataddr default_address
  ,ISNULL(ratepayer_override_address.formataddr,ratepayer_default_address.formataddr) assessment_mailing_address
from 
  pthdbo.crgntry register_entry with (nolock) inner join pthdbo.crgregv notice_group_code with (nolock) on
  (register_entry.tpkcrgntry = notice_group_code.tfkcrgntry
  and notice_group_code.tfkcrgfeld = 953) inner join pthdbo.crgregv name_link with (nolock) on
  (register_entry.tpkcrgntry = name_link.tfkcrgntry
  and name_link.tfkcrgfeld = 954) inner join pthdbo.crgrole agent_role with (nolock) on
  (name_link.tpkcrgregv = agent_role.tfklocl
  and agent_role.fkcrgrolta = 'cna'
  and agent_role.fkcrgroltn = 1
  and agent_role.status = 'c') inner join pthdbo.cnaidty agent with (nolock) on
  (agent.tpkcnaidty = agent_role.tfkappl) inner join pthdbo.cnaidta agent_address_link with (nolock) on
  (agent_address_link.tfkcnaidty = agent.tpkcnaidty
  and not exists (
    select 
	    1 
    from 
	    pthdbo.cnaidta previous_address with (nolock)
    where 
	    previous_address.tfkcnaidty = agent_address_link.tfkcnaidty
	    and previous_address.dateeffect > agent_address_link.dateeffect
    )
  ) inner join pthdbo.cnaaddr agent_address with (nolock) on
  (agent_address.tpkcnaaddr = agent_address_link.tfkcnaaddr) inner join pthdbo.lrantgp notice_group with (nolock) on
  (notice_group.ntgpcode = notice_group_code.regfldvalu) inner join pthdbo.lraassm assessment with (nolock) on
  (notice_group.tpklrantgp = assessment.tfklrantgp) inner join pthdbo.cnarole ratepayer_role with (nolock) on
  (assessment.tpklraassm = ratepayer_role.tfkappl
  and ratepayer_role.fkcnarolta = 'lra'
  and ratepayer_role.fkcnaroltn = 0
  and ratepayer_role.status = 'c') inner join pthdbo.cnaidty ratepayer with (nolock) on
  (ratepayer.tpkcnaidty = ratepayer_role.tfkcnaidty) inner join pthdbo.cnaidta ratepayer_default_address_link with (nolock) on
  (ratepayer_default_address_link.tfkcnaidty = ratepayer.tpkcnaidty
  and not exists (
    select 
	    1 
    from 
	    pthdbo.cnaidta previous_address with (nolock)
    where 
	    previous_address.tfkcnaidty = ratepayer_default_address_link.tfkcnaidty
	    and previous_address.dateeffect > ratepayer_default_address_link.dateeffect
    )) inner join pthdbo.cnaaddr ratepayer_default_address with (nolock) on
  (ratepayer_default_address.tpkcnaaddr = ratepayer_default_address_link.tfkcnaaddr) left outer join pthdbo.cnarola ratepayer_override_address_role with (nolock) on
  (ratepayer_role.tpkcnarole = ratepayer_override_address_role.tfkcnarole
  and ratepayer_override_address_role.isactive = 1
  and not exists (
    select
      1
    from
      pthdbo.cnarola all_ratepayer_override_address with (nolock)
    where
      all_ratepayer_override_address.tfkcnarole = ratepayer_override_address_role.tfkcnarole
      and all_ratepayer_override_address.dateeffect > ratepayer_override_address_role.dateeffect
      and all_ratepayer_override_address.isactive = 1
    )) left outer join pthdbo.cnaaddr ratepayer_override_address on
  (ratepayer_override_address_role.tfkcnaaddr = ratepayer_override_address.tpkcnaaddr)
where 
  register_entry.tfkcrgregs = 84
  and ISNULL(ratepayer_override_address.formataddr,ratepayer_default_address.formataddr) not like '%' + agent_address.formataddr + '%'
  and assessment.status = 'c'