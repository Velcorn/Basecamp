-- Create tables
create table a_documents(
id int primary key,
url varchar,
title varchar,
category text,
comment_count int,
comment_tone json,
answers_tone json);

create table a_comments(
id int primary key,
doc_id int,
user_id int,
parent_comment_id int,
text text,
translation text,
tone json);

create table a_categories(
name varchar primary key,
doc_count int,
comment_tone json,
answers_tone json);

create table a_users(
id int primary key,
comment_count int,
comment_tone json,
answer_tone json,
personality json);

create table a_averages(
name varchar primary key,
comment_tone json,
answer_tone json,
personality json);


-- Create data
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
and (select parent_comment_id from comments pc where id = c.parent_comment_id) is null
and (select pc.text from comments pc where id = c.parent_comment_id) is not null
and length(c.text) >= 100
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
from comments
group by user_id
order by count(user_id) desc
limit 20

insert into a_users(id)
values(%s)
on conflict (id) do nothing

select id, doc_id, user_id, parent_comment_id, text
from comments
where user_id = %s
and parent_comment_id is null
order by length(text) desc
limit 10

select id, doc_id, user_id, parent_comment_id, text
from comments
where user_id = %s
and parent_comment_id is null
and (select parent_comment_id from comments pc where id = parent_comment_id) is null
order by length(text) desc
limit 10

insert into a_comments(id, doc_id, user_id, parent_comment_id, text)
values(%s, %s, %s, %s, %s)
on conflict (id) do nothing

select id, doc_id, user_id, parent_comment_id, text
from comments
where id = %s


-- Analyze data
select id, text
from a_comments
where translation is null

update a_comments
set translation = %s, tone = %s
where id = %s


-- Calc average tone
select id
from a_documents

select tone
from a_comments c
where c.doc_id = %s
and c.parent_comment_id is (not) null

update a_documents
set tone = %s
where id = %s

select name
from a_categories

select tone
from a_documents
where category = %s

update a_categories
set tone = %s
where name = %s

select id
from a_users

select tone
from a_comments
where user_id = %s
and parent_comment_id is null
order by length(text) desc
limit 10

update a_users
set comment_tone = %s
where id = %s


-- Personality insights
select id
from a_users
where personality is null

select distinct translation
from a_comments
join a_users on user_id = %s

update a_users
set personality = %s
where id = %s
