
-- create
CREATE TABLE vwtblposidandetailsumqty (
  fk_prodid INTEGER ,
  batchid TEXT ,
  invqty INTEGER NOT NULL,
  tgl TEXT NOT NULL,
  trans TEXT 
);

-- insert
INSERT INTO vwtblposidandetailsumqty VALUES (1, '1A', 10, '20241101','');
INSERT INTO vwtblposidandetailsumqty VALUES (1, '1A', -5, '20241102','');
INSERT INTO vwtblposidandetailsumqty VALUES (1, '1C', -5, '20241103','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2C', 9, '20241102','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2D', -6, '20241103','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2D', -6, '20241102','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2D', -6, '20241101','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2D', 16, '20241101','');

select x.fk_prodid, x.batchid, x.tgl, x.invqty, x.sumall, sum(coalesce(x.qtybef,0)) as saldoawal, x.trans,x.rn from
(select a.*,b.qtybef from
(select *,row_number() over (partition by fk_prodid,batchid order by fk_prodid,batchid,tgl) as rn ,
sum(invqty) over (partition by fk_prodid,batchid) as sumall
from vwtblposidandetailsumqty) as a left outer join
(select fk_prodid, batchid,tgl,row_number() over (partition by fk_prodid,batchid order by fk_prodid,batchid,tgl) as rn ,
invqty as qtybef from vwtblposidandetailsumqty order by fk_prodid,batchid,tgl) as b 
on a.fk_prodid=b.fk_prodid and a.batchid=b.batchid and a.rn>b.rn) as x 
group by x.fk_prodid, x.batchid, x.tgl, x.invqty, x.sumall,x.trans,x.rn;




CREATE TABLE vwtblposidandetailsumqty (
  fk_prodid INTEGER ,
  batchid TEXT ,
  invqty INTEGER NOT NULL,
  tgl DATE NOT NULL,
  trans TEXT 
);

-- insert
INSERT INTO vwtblposidandetailsumqty VALUES (1, '1A', 10, '20241101','');
INSERT INTO vwtblposidandetailsumqty VALUES (1, '1A', -5, '20241102','');
INSERT INTO vwtblposidandetailsumqty VALUES (1, '1C', -5, '20241103','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2C', 9, '20241102','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2D', -6, '20241103','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2D', -6, '20241102','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2D', -6, '20241101','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2D', 16, '20241101','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2D', -1, '20241031','');
INSERT INTO vwtblposidandetailsumqty VALUES (2, '2D', -2, '20241031','');

select y0.* from 
(select x.fk_prodid, x.batchid, x.tgl, x.invqty, x.sumall, sum(coalesce(x.qtybef,0)) as saldoawal, x.trans,x.rn from
(select a.*,b.qtybef from
(select *,row_number() over (partition by fk_prodid,batchid order by fk_prodid,batchid,tgl) as rn ,
sum(invqty) over (partition by fk_prodid,batchid) as sumall
from vwtblposidandetailsumqty) as a left outer join
(select fk_prodid, batchid,tgl,row_number() over (partition by fk_prodid,batchid order by fk_prodid,batchid,tgl) as rn ,
invqty as qtybef from vwtblposidandetailsumqty order by fk_prodid,batchid,tgl) as b 
on a.fk_prodid=b.fk_prodid and a.batchid=b.batchid and a.rn>b.rn) as x 
where x.tgl < '2024-11-01' group by x.fk_prodid, x.batchid, x.tgl, x.invqty, x.sumall,x.trans,x.rn) as y0 union
select y.* from 
(select x.fk_prodid, x.batchid, x.tgl, x.invqty, x.sumall, sum(coalesce(x.qtybef,0)) as saldoawal, x.trans,x.rn from
(select a.*,b.qtybef from
(select *,row_number() over (partition by fk_prodid,batchid order by fk_prodid,batchid,tgl) as rn ,
sum(invqty) over (partition by fk_prodid,batchid) as sumall
from vwtblposidandetailsumqty) as a left outer join
(select fk_prodid, batchid,tgl,row_number() over (partition by fk_prodid,batchid order by fk_prodid,batchid,tgl) as rn ,
invqty as qtybef from vwtblposidandetailsumqty order by fk_prodid,batchid,tgl) as b 
on a.fk_prodid=b.fk_prodid and a.batchid=b.batchid and a.rn>b.rn) as x 
where x.tgl between '2024-11-01' and '2024-11-30' group by x.fk_prodid, x.batchid, x.tgl, x.invqty, x.sumall,x.trans,x.rn) as y 
;
