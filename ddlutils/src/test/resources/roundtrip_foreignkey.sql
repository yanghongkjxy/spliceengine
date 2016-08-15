-- -----------------------------------------------------------------------
--  SCHEMA ROUNDTRIP_FOREIGNKEY
-- -----------------------------------------------------------------------

CREATE SCHEMA ROUNDTRIP_FOREIGNKEY AUTHORIZATION SPLICE;

SET SCHEMA ROUNDTRIP_FOREIGNKEY;

-- -----------------------------------------------------------------------
-- TABLE ROUNDTRIP_FOREIGNKEY.C1
-- -----------------------------------------------------------------------

CREATE TABLE ROUNDTRIP_FOREIGNKEY.C1
(
    C INTEGER,
    F INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE ROUNDTRIP_FOREIGNKEY.C2
-- -----------------------------------------------------------------------

CREATE TABLE ROUNDTRIP_FOREIGNKEY.C2
(
    G INTEGER check(G>0),
    H INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE ROUNDTRIP_FOREIGNKEY.P
-- -----------------------------------------------------------------------

CREATE TABLE ROUNDTRIP_FOREIGNKEY.P
(
    A INTEGER NOT NULL,
    B INTEGER unique,
    PRIMARY KEY (A)
);

ALTER TABLE ROUNDTRIP_FOREIGNKEY.C1
    ADD CONSTRAINT C1_FK FOREIGN KEY (F) REFERENCES ROUNDTRIP_FOREIGNKEY.P (A);

ALTER TABLE ROUNDTRIP_FOREIGNKEY.C2
    ADD CONSTRAINT C2_FK FOREIGN KEY (H) REFERENCES ROUNDTRIP_FOREIGNKEY.P (B);

call SYSCS_UTIL.IMPORT_DATA ('ROUNDTRIP_FOREIGNKEY', 'P', null, '<USER.DIR>/src/test/resources/foreign_key/p.csv', ',', '"', null,null,null,0,null,true,null);
call SYSCS_UTIL.IMPORT_DATA ('ROUNDTRIP_FOREIGNKEY', 'C1', null, '<USER.DIR>/src/test/resources/foreign_key/c1.csv', ',', '"', null,null,null,0,null,true,null);
call SYSCS_UTIL.IMPORT_DATA ('ROUNDTRIP_FOREIGNKEY', 'C2', null, '<USER.DIR>/src/test/resources/foreign_key/c2.csv', ',', '"', null,null,null,0,null,true,null);
