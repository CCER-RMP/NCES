
-- Find any 'bad' values, usually due to parsing problems in early years,
-- when files have fixed-length records

select *
from {{ ref('final') }} where
    not (charter is null OR charter in ('Yes', 'No'))
    or not (magnet is null OR Magnet in ('Yes', 'No'))
    or not (TitleISchool is null OR TitleISchool in ('Yes', 'No'))
    or not (TitleISchoolWide is null OR TitleISchoolWide in ('Yes', 'No'))
    OR (students is not null and cast(students as float) < 0.0)
    OR (teachers is not null and cast(teachers as float) < 0.0)
    OR (freelunch is not null and cast(freelunch as float) < 0.0)
    or (reducedlunch is not null and cast(Reducedlunch as float) < 0.0)
