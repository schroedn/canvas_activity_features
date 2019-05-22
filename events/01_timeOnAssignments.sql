--Calculate time on all assignments
WITH
  assignmentEvents as (
  SELECT
    case when json_extract_scalar(event,"$[federatedSession][messageParameters][custom_canvas_user_id]") is not null 
        then json_extract_scalar(event,"$[federatedSession][messageParameters][custom_canvas_user_id]") else actor_id end 
    as user_id,
    case 
        when substr(json_extract_scalar(event,"$[object][assignable][id]"),24,10)='assignment' then 
        substr(json_extract_scalar(event,"$[object][assignable][id]"),-7,7) 
        when substr(json_extract_scalar(event,"$[object][id]"),24,10)='assignment' then 
        substr(json_extract_scalar(event,"$[object][id]"),-7,7)  
        when json_extract_scalar(event,"$[federatedSession][messageParameters][custom_canvas_assignment_id]") is not null 
        then json_extract_scalar(event,"$[federatedSession][messageParameters][custom_canvas_assignment_id]")
    end 
    as assignment_id,
    SUBSTR(JSON_EXTRACT_SCALAR(event,"$[session][id]"),-32,32) AS session_id,
    TIMESTAMP_DIFF(LEAD(event_time) OVER (PARTITION BY case when json_extract_scalar(event,"$[federatedSession][messageParameters][custom_canvas_user_id]") is not null 
        then json_extract_scalar(event,"$[federatedSession][messageParameters][custom_canvas_user_id]") else actor_id end ORDER BY event_time),
      event_time,
      second) AS timebetweenEvents,
    case when json_extract_scalar(event,"$[federatedSession][messageParameters][custom_canvas_course_id]") is not null then json_extract_scalar(event,"$[federatedSession][messageParameters][custom_canvas_course_id]")
        when substr(json_extract_scalar(event,"$[group][id]"),24,6) ='course' then substr(json_extract_scalar(event,"$[group][id]"),39,7) 
        when substr(json_extract_scalar(event,"$[object][id]"),24,6) ='course' then substr(json_extract_scalar(event,"$[object][id]"),39,7) 
        end 
    as course_id
  FROM
    `udp-iu-prod.event_store.events`
  WHERE
    EXTRACT(DATE FROM event_time) BETWEEN '2019-04-07' AND '2019-04-14' limit 10000)
    
    
    select course_id,user_id,sum(timebetweenEvents) as timeOnAssignments 
    from assignmentEvents
    where timebetweenEvents <=600 and assignment_id is not null
    group by course_id, user_id
    
