select count(md.id) from mf_data md
join mf_risk mr on md.id = mr.mf_id and md.time_added = mr.time_added
join mf_growth mg on md.id = mg.mf_id and md.time_added = mg.time_added
join mf_credit_rating mcr on md.id = mcr.mf_id and md.time_added = mcr.time_added
join mf_category_data mcd on md.category_id = mcd.id
join mf_category_growth mcg on mcd.id = mcg.mf_cat_id
join mf_category_risk m on mcd.id = m.mf_cat_id
where md.time_added='2023-01-12'
group by md.id, md.time_added;

 
select * from mf_growth mg  where mg.mf_id='3389' and mg.time_added='2023-01-16';

SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));



select * from (select 
'3m' as mf_type ,mg.3m as mf_growth, mcg.3m as mf_cat_growth, md.time_added as time_added
from mf_growth mg 
join mf_data md on md.id=mg.mf_id and md.time_added =mg.time_added 
join mf_category_growth mcg on mcg.mf_cat_id = md.category_id and mcg.time_added = mg.time_added 
where mg.mf_id ='3389' 
union all
select 
'6m' as mf_type ,mg.6m as mf_growth, mcg.6m as mf_cat_growth,md.time_added as time_added
from mf_growth mg 
join mf_data md on md.id=mg.mf_id and md.time_added =mg.time_added 
join mf_category_growth mcg on mcg.mf_cat_id = md.category_id and mcg.time_added = mg.time_added 
where mg.mf_id ='3389' 
union all
select 
'1y' as mf_type ,mg.1y as mf_growth, mcg.1y as mf_cat_growth,md.time_added as time_added
from mf_growth mg 
join mf_data md on md.id=mg.mf_id and md.time_added =mg.time_added 
join mf_category_growth mcg on mcg.mf_cat_id = md.category_id and mcg.time_added = mg.time_added 
where mg.mf_id ='3389' 
union all
select 
'3y' as mf_type ,mg.3y as mf_growth, mcg.3y as mf_cat_growth,md.time_added as time_added
from mf_growth mg 
join mf_data md on md.id=mg.mf_id and md.time_added =mg.time_added 
join mf_category_growth mcg on mcg.mf_cat_id = md.category_id and mcg.time_added = mg.time_added 
where mg.mf_id ='3389' 
union all
select 
'5y' as mf_type ,mg.5y as mf_growth, mcg.5y as mf_cat_growth,md.time_added as time_added
from mf_growth mg 
join mf_data md on md.id=mg.mf_id and md.time_added =mg.time_added 
join mf_category_growth mcg on mcg.mf_cat_id = md.category_id and mcg.time_added = mg.time_added 
where mg.mf_id ='3389' 
union all
select 
'7y' as mf_type ,mg.7y as mf_growth, mcg.7y as mf_cat_growth,md.time_added as time_added
from mf_growth mg 
join mf_data md on md.id=mg.mf_id and md.time_added =mg.time_added 
join mf_category_growth mcg on mcg.mf_cat_id = md.category_id and mcg.time_added = mg.time_added 
where mg.mf_id ='3389' 
union ALL 
SELECT 
'10y' as mf_type ,mg.10y as mf_growth, mcg.10y as mf_cat_growth,md.time_added as time_added
from mf_growth mg 
join mf_data md on md.id=mg.mf_id and md.time_added =mg.time_added 
join mf_category_growth mcg on mcg.mf_cat_id = md.category_id and mcg.time_added = mg.time_added 
where mg.mf_id ='3389') a 
where a.time_added = '2023-01-16' 
order by FIELD(mf_type, '3m','6m','1y','3y','5y','7y','10y') 