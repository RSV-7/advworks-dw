{{
    config(
        unique_key='employee_id', 
        strategy='check', 
        check_cols=['national_nr_id', 'login_id', 'email'] 
    )
}}

select
    e.businessentityid as employee_id,
    e.nationalidnumber as national_nr_id,
    e.loginid as login_id,
    p.firstname as first_name,
    p.middlename as middle_name,
    p.lastname as last_name,
    email.emailaddress as email,
    d.name as department_name,
    e.jobtitle as job_title,
    e.organizationnode as organization_node,
    greatest(edh.modifieddate,e.modifieddate, p.modifieddate) as employee_last_update
from {{ source("humanresources", "employeedepartmenthistory") }} edh
    inner join {{ source("humanresources", "department") }} d on edh.departmentid = d.departmentid
    inner join {{ source("humanresources", "employee") }} e on edh.businessentityid = e.businessentityid
    inner join {{ source("person", "person") }} p on e.businessentityid = p.businessentityid
    inner join {{ source("person", "emailaddress") }} email on p.businessentityid = email.businessentityid
where (edh.businessentityid, edh.modifieddate) in (
    select businessentityid, max(modifieddate)
    from {{ source("humanresources", "employeedepartmenthistory") }}
    group by businessentityid
)
