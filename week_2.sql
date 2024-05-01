with survey_active_food_count as (
    select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
    ),

chicago_geo_loc as (
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL'
    ),

gary_geo_loc as (
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN'
    )

select 
    first_name || ' ' || last_name as customer_name,
    ca.customer_city,
    ca.customer_state,
    safc.food_pref_count,
    (st_distance(us.geo_location, chicago_geo_loc.geo_location) / 1609)::int as chicago_distance_miles,
    (st_distance(us.geo_location, gary_geo_loc.geo_location) / 1609)::int as gary_distance_miles
from vk_data.customers.customer_address as ca
inner join vk_data.customers.customer_data c
    on ca.customer_id = c.customer_id
left join vk_data.resources.us_cities us 
    on upper(rtrim(ltrim(ca.customer_state))) = upper(trim(us.state_abbr))
    and trim(lower(ca.customer_city)) = trim(lower(us.city_name))
inner join survey_active_food_count safc
    on c.customer_id = safc.customer_id
cross join chicago_geo_loc
cross join gary_geo_loc
where true
    and ((trim(city_name) ilike '%concord%' or trim(city_name) ilike '%georgetown%' or trim(city_name) ilike '%ashland%')
    and customer_state = 'KY')
    or customer_state = 'CA' and (trim(city_name) ilike '%oakland%' or trim(city_name) ilike '%pleasant hill%')
    or (customer_state = 'TX' and ((trim(city_name) ilike '%arlington%') or trim(city_name) ilike '%brownsville%'))
order by 5 desc, 6 desc;
