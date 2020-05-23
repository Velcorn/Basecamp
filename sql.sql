select id, url, title from documents d
where metadata like '%"channel": "Netzwelt"%'

select c.id, c.doc_id, parent_comment_id, c."text", "year", "month", "day"
from "comments" c
join documents
on documents.id = c.doc_id
where metadata like '%"channel": "Netzwelt"%'