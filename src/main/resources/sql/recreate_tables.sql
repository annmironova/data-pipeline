drop schema if exists test;
create schema if not exists test;

create table test.uc_news (
                         ID int,
                         TITLE varchar(256),
                         URL varchar(256),
                         PUBLISHER varchar(256),
                         CATEGORY varchar(256),
                         STORY varchar(256),
                         HOSTNAME varchar(256),
                         ts timestamp
);