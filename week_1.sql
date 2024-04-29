with cities as (

    select
        lower(trim(city_name)) as city_name,
        lower(trim(state_name)) as state_name,
        lower(trim(state_abbr)) as state_abbr,
        geo_location
    from vk_data.resources.us_cities
    qualify row_number() over (partition by lower(trim(city_name)), lower(trim(state_abbr)) order by city_name) = 1
    
    ),

suppliers_geo_location as (
    select
        supplier_id,
        supplier_name,
        lower(trim(si.supplier_city)) as supplier_city,
        lower(trim(si.supplier_state)) as supplier_state,
        c.geo_location as supplier_geo_location
    from vk_data.suppliers.supplier_info as si
    inner join cities as c
        on lower(trim(si.supplier_city)) = c.city_name
        and lower(trim(si.supplier_state)) = c.state_abbr
        
        ),

customer_geo_location as (
    select
        ca.customer_id,
        cd.first_name as customer_first_name,
        cd.last_name as customer_last_name,
        cd.email as customer_email,
        lower(trim(ca.customer_city)) as customer_city,
        lower(trim(ca.customer_state)) as customer_state,
        c.geo_location as customer_geo_location
    from vk_data.customers.customer_address as ca
    inner join vk_data.customers.customer_data cd
        using (customer_id)
    inner join cities as c
        on lower(trim(ca.customer_city)) = lower(c.city_name)
        and lower(trim(ca.customer_state)) = lower(c.state_abbr)

    ),

shipping_distance as (
    select
        cg.customer_id,
        cg.customer_first_name,
        cg.customer_last_name,
        cg.customer_email,
        sg.supplier_id,
        sg.supplier_name,
        st_distance(cg.customer_geo_location, sg.supplier_geo_location) / 1000 as shipping_distance
    from customer_geo_location as cg
    cross join suppliers_geo_location as sg
    qualify row_number() over (partition by customer_id order by shipping_distance asc) = 1
    
)


select
    customer_id as "Customer ID",
    customer_first_name as "Customer first name",
    customer_last_name as "Customer last name",
    customer_email as "Customer email",
    supplier_id as "Supplier ID",
    supplier_name as "Supplier name",
    shipping_distance as "Shipping distance"
from shipping_distance
order by 3,2;
