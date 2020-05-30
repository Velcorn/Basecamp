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
tone text);

create table a_categories(
name varchar primary key,
doc_count int,
comment_count int,
tone text);

create table a_users(
id int primary key,
comment_count int,
tone text,
personality text);

select distinct c.year, c.month, c.day
from comments c
join documents
on doc_id = documents.id
where user_id is not null
group by c.year, c.month, c.day

select distinct d.id, d.url, d.title, count(comments)
from documents d
join comments
on d.id = comments.doc_id
where metadata like '%"channel": "category"%'
and to_char(d.timestamp, 'YYYY-MM-DD') like day
and comments.user_id is not null
group by d.id
order by count(comments) desc
limit 1

insert into a_documents(id, url, title, category, comment_count)
values(%s, %s, %s, %s, %s)
on conflict do nothing

select distinct on (parent_comment_id) c.id, doc_id, user_id, parent_comment_id, c.text
from comments c
join a_documents
on doc_id = a_documents.id
where user_id is not null
and parent_comment_id is not null
and (select parent_comment_id from comments p where id = c.parent_comment_id) is null
order by parent_comment_id, c.id asc
limit 10

insert into a_comments(id, doc_id, user_id, parent_comment_id, text)
values(%s, %s, %s, %s, %s)
on conflict do nothing

select c.id, c.doc_id, c.user_id, c.parent_comment_id, c.text
from comments c
join a_comments
on c.id = a_comments.parent_comment_id
where c.doc_id = %s
order by c.id asc
limit 10

select count(id), sum(comment_count)
from a_documents d
where category = 'category'

insert into a_categories(name, doc_count, comment_count)
values(%s, %s, %s)
on conflict (name) do update
set (doc_count, comment_count) = (EXCLUDED.doc_count, EXCLUDED.comment_count)

select user_id, count(user_id)
from a_comments c
group by user_id
order by count(user_id) desc
limit 10

insert into a_users(id, comment_count)
values(%s, %s)
on conflict (id) do update
set comment_count = EXCLUDED.comment_count

select text
from a_comments
where translation is null
order by id asc

update a_comments
set translation = %s, tone = %s
where id = %s
