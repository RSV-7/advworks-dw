with base_data as (
    select
        employee_id,
        national_nr_id,
        login_id,
        first_name,
        middle_name,
        last_name,
        email,
        department_name,
        job_title,
        organization_node,
        employee_last_update as dbt_updated_at
    from {{ ref("stg_employee") }}
),
scd_data as (
    select
        row_number() over() as dbt_scd_id, 
        employee_id,
        national_nr_id,
        login_id,
        first_name,
        middle_name,
        last_name,
        email,
        department_name,
        job_title,
        organization_node,
        dbt_updated_at,
        row_number() over(partition by employee_id order by dbt_updated_at) as row_nr
    from base_data
)
select
    dbt_scd_id as sk_employee,
    employee_id,
    national_nr_id,
    login_id,
    first_name,
    middle_name,
    last_name,
    email,
    department_name,
    job_title,
    organization_node,
    case
        when row_nr = 1 then '1970-01-01'
        else dbt_updated_at
    end as valid_from,
    coalesce(null, '2200-01-01') as valid_to, 
    dbt_updated_at as last_updated_at
from scd_data


