
-- 作业1


-- 查询"01"课程比"02"课程成绩高的学生的信息及课程分数
select s.s_id AS ID, s_name AS 姓名, s_birth AS 生日, s_sex AS 性别, c1.c_id AS 科目1, c1.s_score AS 成绩1, c2.c_id AS 科目2, c2.s_score AS 成绩2
from (select s_id, c_id, s_score from score s where c_id = '01') c1
    left join (select s_id, c_id, s_score from score s where c_id = '02') c2 on c1.s_id = c2.s_id
    left join student s on s.s_id = c1.s_id
where c1.s_score > c2.s_score;

| ID   | 姓名 | 出生日期  | 性别  | 科目1 | 成绩1 | 科目2 | 成绩2 |
|------|------|----------|-----|------|------|------|-------|
| 02   | 钱电 | 1990-12-21 | 男  | 01   | 70   | 02   | 60   |
| 04   | 李云 | 1990-08-06 | 男  | 01   | 50   | 02   | 30   |
| 09   | 张飞 | 1990-09-25 | 男  | 01   | 85   | 02   | 80   |
| 10   | 刘备 | 1990-01-25 | 男  | 01   | 80   | 02   | 56   |


-- 查询"01"课程比"02"课程成绩低的学生的信息及课程分数

select s.s_id AS ID, s_name AS 姓名, s_birth AS 生日, s_sex AS 性别, c1.c_id AS 学科1, c1.s_score AS 成绩1, c2.c_id AS 学科2, c2.s_score AS 成绩2
from (select s_id, c_id, s_score from score s where c_id = '01') c1
    left join (select s_id, c_id, s_score from score s where c_id = '02') c2 on c1.s_id = c2.s_id
    left join student s on s.s_id = c1.s_id
where c1.s_score < c2.s_score;

| ID  | 姓名 | 出生日期    | 性别 |  学科1 | 成绩1 | 学科2 | 成绩2 |
|------|------|---------- |------|-------|-------|-------|-------|
| 01   | 赵雷 | 1990-01-01 | 男   | 01    | 80    | 02    | 90    |
| 05   | 周梅 | 1991-12-01 | 女   | 01    | 76    | 02    | 87    |


-- 查询平均成绩大于等于 60 分的同学的学生编号和学生姓名和平均成绩

select s.s_id 编号, s2.s_name AS 姓名, avg(s_score) AS 平均成绩
from score s
    left join student s2 on s.s_id = s2.s_id
group by s.s_id, s2.s_name
having avg(s_score) >= 60;

| 编号 | 姓名 | 平均成绩 |
| --- | --- | --- |
| 01 | 赵雷 | 89.6667 |
| 02 | 钱电 | 70.0000 |
| 03 | 孙风 | 80.0000 |
| 05 | 周梅 | 81.5000 |
| 07 | 郑竹 | 93.5000 |
| 09 | 张飞 | 88.0000 |
| 10 | 刘备 | 64.0000 |
| 11 | 关羽 | 90.0000 |


-- 查询平均成绩小于 60 分的同学的学生编号和学生姓名和平均成绩 (包括有成绩的和无成绩的)

select s2.s_id 编号, s2.s_name AS 姓名, avg(s_score) AS 平均成绩
from student s2
    left join score s  on s.s_id = s2.s_id
group by s2.s_id, s2.s_name
having avg(s_score) < 60  or avg(s_score)  is null;

| 序号 | 姓名 | 分数 |
| --- | --- | --- |
| 04 | 李云 | 33.3333 |
| 06 | 吴兰 | 32.5000 |
| 08 | 王菊 | NULL |


-- 查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩

select s2.s_id 编号, s2.s_name AS 姓名, count(distinct s.c_id) AS 选课总数, sum(s.s_score) AS 总成绩
from student s2
    left join score s  on s.s_id = s2.s_id
group by s2.s_id, s2.s_name
;

| 编号 | 姓名 | 选课总数 | 总成绩 |
| --- | --- | --- | --- |
| 01 | 赵雷 | 3 | 269 |
| 02 | 钱电 | 3 | 210 |
| 03 | 孙风 | 3 | 240 |
| 04 | 李云 | 3 | 100 |
| 05 | 周梅 | 2 | 163 |
| 06 | 吴兰 | 2 | 65 |
| 07 | 郑竹 | 2 | 187 |
| 08 | 王菊 | 0 | NULL |
| 09 | 张飞 | 3 | 264 |
| 10 | 刘备 | 4 | 256 |
| 11 | 关羽 | 1 | 90 |


-- 查询"李"姓老师的数量

select count(1) 李姓老师的数量 from teacher where t_name like '李%';

| 李姓老师的数量 |
| ------------ |
|      1       |


-- 查询学过"张三"老师授课的同学的信息

select s.s_id AS 编号, s2.s_name AS 姓名, s_birth AS 出生日期, s_sex AS 性别
from score s
    left join course c on s.c_id = c.c_id
    left join teacher t on c.t_id = t.t_id
    left join student s2 on s.s_id = s2.s_id
where t_name = '张三'
group by s.s_id,s2.s_name, s_birth, s_sex ;

｜ 编号 | 姓名 | 出生日期 | 性别
｜-----|------|---------|-----
｜01 | 赵雷 | 1990-01-01 | 男
｜02 | 钱电 | 1990-12-21 | 男
｜03 | 孙风 | 1990-05-20 | 男
｜04 | 李云 | 1990-08-06 | 男
｜05 | 周梅 | 1991-12-01 | 女
｜07 | 郑竹 | 1989-07-01 | 女
｜09 | 张飞 | 1990-09-25 | 男
｜10 | 刘备 | 1990-01-25 | 男



-- 查询没学过"张三"老师授课的同学的信息

select s.s_id AS 编号, s2.s_name AS 姓名, s_birth AS 出生日期, s_sex AS 性别
from score s
    left join course c on s.c_id = c.c_id
    left join teacher t on c.t_id = t.t_id
    left join student s2 on s.s_id = s2.s_id
group by s.s_id,s2.s_name, s_birth, s_sex
having group_concat(distinct t.t_name) not like '%张三%';


| 编号 | 姓名 | 出生日期    | 性别 |
|------|------|----------|------|
| 06   | 吴兰 | 1992-03-01 | 女   |
| 11   | 关羽 | 1990-01-25 | 男   |



-- 查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息

select s.s_id AS 编号, s2.s_name AS 姓名, s_birth AS 出生日期, s_sex AS 性别 from score s
left join student s2 on s.s_id = s2.s_id
where s.s_id in (select s_id from score where c_id = '01')
and s.c_id = '02';


| 编号 | 姓名 | 出生日期   | 性别 |
|-----|------|----------|------|
| 01  | 赵雷 | 1990-01-01 | 男 |
| 02  | 钱电 | 1990-12-21 | 男 |
| 03  | 孙风 | 1990-05-20 | 男 |
| 04  | 李云 | 1990-08-06 | 男 |
| 05  | 周梅 | 1991-12-01 | 女 |
| 09  | 张飞 | 1990-09-25 | 男 |
| 10  | 刘备 | 1990-01-25 | 男 |


-- 查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息

select s.s_id AS 编号, s2.s_name AS 姓名, s_birth AS 出生日期, s_sex AS 性别
 from score s
left join student s2 on s.s_id = s2.s_id
where s.s_id in (select s_id from score where c_id = '01')
group by s.s_id, s2.s_name, s_birth, s_sex
having group_concat(c_id) not like '%02%';

| 编号 | 姓名 | 出生日期   | 性别 |
|-----|------|----------|------|
| 06  | 吴兰 | 1992-03-01 | 女 |


-- 查询没有学全所有课程的同学的信息

select s2.s_id AS 编号, s2.s_name AS 姓名, s_birth AS 出生日期, s_sex AS 性别
 from student s2
left join score s  on s.s_id = s2.s_id
group by s2.s_id, s2.s_name, s_birth, s_sex
having count(ifnull(c_id, 0)) < 4
;


| 编号 | 姓名  | 出生日期   | 性别 |
|------|------|----------|------|
| 01   | 赵雷 | 1990-01-01 | 男   |
| 02   | 钱电 | 1990-12-21 | 男   |
| 03   | 孙风 | 1990-05-20 | 男   |
| 04   | 李云 | 1990-08-06 | 男   |
| 05   | 周梅 | 1991-12-01 | 女   |
| 06   | 吴兰 | 1992-03-01 | 女   |
| 07   | 郑竹 | 1989-07-01 | 女   |
| 08   | 王菊 | 1990-01-20 | 女   |
| 09   | 张飞 | 1990-09-25 | 男   |
| 11   | 关羽 | 1990-01-25 | 男   |



-- 查询至少有一门课与学号为"01"的同学所学相同的同学的信息

select distinct s2.s_id AS 编号, s2.s_name AS 姓名, s_birth AS 出生日期, s_sex AS 性别
from score s
    left join student s2 on s.s_id = s2.s_id
where c_id in
      (select c_id from score where s_id = '01')
and s.s_id <> '01';


| 编号 | 姓名 | 出生日期   | 性别 |
|------|------|------------|------|
| 02   | 钱电 | 1990-12-21 | 男   |
| 03   | 孙风 | 1990-05-20 | 男   |
| 04   | 李云 | 1990-08-06 | 男   |
| 05   | 周梅 | 1991-12-01 | 女   |
| 06   | 吴兰 | 1992-03-01 | 女   |
| 07   | 郑竹 | 1989-07-01 | 女   |
| 09   | 张飞 | 1990-09-25 | 男   |
| 10   | 刘备 | 1990-01-25 | 男   |


-- 查询和"01"号的同学学习的课程完全相同的其他同学的信息

select s2.s_id AS 编号, s2.s_name AS 姓名, s_birth AS 出生日期, s_sex AS 性别
from score s
    left join student s2 on s.s_id = s2.s_id
where s.s_id <> '01'
    group by s2.s_id, s_name, s_birth, s_sex
having group_concat(c_id) = (select group_concat(c_id) from score where s_id = '01');


| 编号 | 姓名 | 出生日期 | 性别 |
|-----|-----|----------|-----|
| 02 | 钱电 | 1990-12-21 | 男 |
| 03 | 孙风 | 1990-05-20 | 男 |
| 04 | 李云 | 1990-08-06 | 男 |


-- 查询没学过"张三"老师讲授的任一门课程的学生姓名

select s.s_id AS 编号, s2.s_name AS 姓名
from score s
    left join course c on s.c_id = c.c_id
    left join teacher t on c.t_id = t.t_id
    left join student s2 on s.s_id = s2.s_id
group by s.s_id,s2.s_name, s_birth, s_sex
having group_concat(distinct t.t_name) not like '%张三%';


| 编号 | 姓名 |
|------|------|
| 06   | 吴兰 |
| 11   | 关羽 |


-- 查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩

select s.s_id AS 编号, s_name AS 姓名, avg(s_score) AS 平均成绩 from score s
    left join student s2 on s.s_id = s2.s_id
where s.s_score < 60
group by s.s_id, s_name
having count(s.s_id) >= 2;


| 序号 | 姓名 | 年龄 |
|------|------|------|
| 04   | 李云 | 33.3333 |
| 06   | 吴兰 | 32.5000 |
| 10   | 刘备 | 43.0000 |

