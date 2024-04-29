with distinct_city_state as (
    select distinct 
        lower(trim(city_name)) as city_name, 
        lower(trim(state_abbr)) as state_abbr,
        geo_location
    from vk_data.resources.us_cities),

customers_location as
    (
    select 
        cd.customer_id,
        cd.first_name,
        cd.last_name,
        cd.email,
        dcs.city_name,
        dcs.state_abbr,
        dcs.geo_location as customer_geo_location
    from vk_data.customers.customer_data as cd
    inner join vk_data.customers.customer_address as ca
        on cd.customer_id = ca.customer_id
    inner join distinct_city_state as dcs
        on lower(trim(ca.customer_city)) = dcs.city_name and lower(trim(ca.customer_state)) = dcs.state_abbr), 
        
suppliers_location as
(
    select 
        si.supplier_id,
        si.supplier_name,
        dcs.city_name,
        dcs.state_abbr,
        dcs.geo_location as supplier_geo_location
    from vk_data.suppliers.supplier_info as si
    left join distinct_city_state as dcs
        on lower(trim(si.supplier_city)) = dcs.city_name and lower(trim(si.supplier_state)) = dcs.state_abbr),
        
customers_suppliers_distance as (
    select
        cl.customer_id,
        cl.first_name,
        cl.last_name,
        cl.email,
        sl.supplier_id,
        sl.supplier_name,
        st_distance(cl.customer_geo_location, sl.supplier_geo_location) / 1609 as distance_to_supplier_miles
    from customers_location as cl
    cross join suppliers_location as sl
    qualify row_number() over (partition by cl.customer_id order by distance_to_supplier_miles) = 1
    order by cl.last_name, cl.first_name),

customer_preference_tags as (
    select 
        csd.first_name,
        csd.email,
        csd.customer_id,
        rt.tag_property,
        row_number() over (partition by cs.customer_id order by rt.tag_property) as tag_id
    from customers_suppliers_distance as csd
    inner join vk_data.customers.customer_survey as cs
        on csd.customer_id = cs.customer_id 
    inner join vk_data.resources.recipe_tags as rt
        on cs.tag_id = rt.tag_id
        where cs.is_active = 'TRUE'),


customer_flatten_tags as (        
    select
        *
    from customer_preference_tags
    pivot(min(tag_property) for tag_id in (1, 2, 3))
        as p(first_name, email, customer_id, food_pref_1, food_pref_2, food_pref_3)
),

recipe_tags as(
    select 
        recipe_id,
        recipe_name as suggested_recipe,
        trim(replace(flat_tag.value, '"', '')) as recipe_tag,
        row_number() over (partition by recipe_tag order by recipe_id, recipe_name) as recipe_property_tag_id
    from vk_data.chefs.recipe
    , table(flatten(tag_list)) as flat_tag)

select
    cft.first_name,
    cft.email,
    cft.customer_id,
    cft.food_pref_1,
    cft.food_pref_2,
    cft.food_pref_3,
    rec_tags.suggested_recipe
from customer_flatten_tags as cft
join recipe_tags as rec_tags
    on lower(trim(cft.food_pref_1)) = lower(trim(rec_tags.recipe_tag))
    where recipe_property_tag_id = 1
    order by email
