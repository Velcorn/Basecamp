create table a_documents(
id int primary key,
url varchar,
title varchar,
category text,
comment_count int,
tone text);

create table a_comments(
id int primary key,
doc_id int,
user_id int,
parent_comment_id int,
text text,
translation text,
tone text,
year int,
month int,
day int);

create table a_categories(
name varchar,
tone text);

create table a_users(
id int,
comment_count int,
tone text,
personality text);

select distinct d.id, d.url, d.title
from documents d
join comments
on d.id = comments.doc_id
where metadata like '%"channel": "Channel"%'
and comments.user_id is not null
order by id asc

select c.id, doc_id, user_id, parent_comment_id, c."text", "year", "month", "day"
from comments c
join documents on documents.id = c.doc_id
where metadata like '%"channel": "Channel"%'
and user_id is not null
order by c.id asc

insert into a_documents(id, url, title, category)
values(%s, %s, %s, %s)
on conflict do nothing

insert into a_comments(id, doc_id, user_id, parent_comment_id, text, year, month, day)
values(%s, %s, %s, %s, %s, %s, %s, %s)
on conflict do nothing
