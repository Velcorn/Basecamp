create table documents(
id int primary key,
url varchar,
title text,
category text,
"comments" int,
tone text)

create table comments(
id int primary key,
doc_id int,
parent_comment_id int,
"text" text,
"translation" text,
tone text,
year int,
month int,
day int)

select id, url, title from documents d
where metadata like '%"channel": "Netzwelt"%'

select c.id, c.doc_id, parent_comment_id, c."text", "year", "month", "day"
from "comments" c
join documents on documents.id = c.doc_id
where metadata like '%"channel": "Netzwelt"%'
