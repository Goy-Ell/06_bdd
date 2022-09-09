drop database if exists joconde;

create database if not exists joconde;
use joconde;



CREATE TABLE IF NOT EXISTS musee(
  id        varchar(100) primary key ,
  nom       varchar(100),
  # pays 	  varchar(100),
  region    varchar(100),
  dept      varchar(100),
  ville     varchar(100),
  geoloc    varchar(100)
  # plaquette varchar(100)
) ENGINE InnoDB;

CREATE TABLE IF NOT EXISTS oeuvre(
  id            varchar(100) primary key ,
  nom           varchar(100),
  denomination  varchar(100),
  sujet         varchar(100),
  domaine       varchar(100),
  musee         varchar(100),
  foreign key(musee) references musee(id)
) ENGINE InnoDB;


CREATE TABLE IF NOT EXISTS artiste(
  id        int primary key auto_increment,
  nom       varchar(200) NOT NULL,
  naissance date,
  lieu_n    varchar(100),
  mort      date,
  lieu_m    varchar(100),
  UNIQUE(nom)
) ENGINE InnoDB;


CREATE TABLE IF NOT EXISTS art_oeuv(
  oeuvre       varchar(100) ,
  artiste     int ,
  rol       varchar(100),
  primary key (oeuvre,artiste),
  foreign key (oeuvre) references oeuvre(id),
  foreign key (artiste) references artiste(id)
) ENGINE InnoDB;

INSERT INTO artiste (nom) VALUES ('Non Renseigné')