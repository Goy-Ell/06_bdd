drop database if exists `magicien`;

create database `magicien`;

use `magicien`;

DROP TABLE IF EXISTS `joueur`;

CREATE TABLE `joueur` (
  `pseudo` varchar(50) NOT NULL,
  `nom` varchar(50) DEFAULT NULL,
  `prenom` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`pseudo`)
) ;

INSERT INTO `joueur` VALUES 
('georges','LEBRETON','Georges'),
('stifo22','TOTO','Toto'),
('super_tatasse','LABROSSE','Adan');

DROP TABLE IF EXISTS `terrain`;

CREATE TABLE `terrain` (
  id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
  nom VARCHAR(50) NOT NULL,
  type_terrain VARCHAR(50) NOT NULL,
  PRIMARY KEY(id)
);

INSERT INTO `terrain` VALUES 
(1,'MARECAGE','MARECAGE'),
(2,'MENEZ-BRE','MONTAGNE'),
(3,'CHAMP ELYSEE','PLAINE'),
(4,'SAHARA','DESERT');

DROP TABLE IF EXISTS `classe`;

CREATE TABLE `classe` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `type_classe` varchar(50) NOT NULL,
  `jauge_magie_initiale` int unsigned NOT NULL,
  `jauge_vie_initiale` int unsigned NOT NULL,
  `sac_capacite_initiale` int unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ;

INSERT INTO `classe` VALUES 
(1,'DRUIDE',100,50,5),
(2,'MAGE',120,20,4),
(3,'NECROMANCIEN',80,100,9),
(4,'DRAGON',90,70,10),
(5,'CODINCORNE',1,5000,1);


DROP TABLE IF EXISTS `sort`;

CREATE TABLE `sort` (
  id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
  nom VARCHAR(50) NOT NULL,
  type_sort VARCHAR(50) NOT NULL,
  cout_magie INTEGER UNSIGNED NOT NULL,
  type_terrain VARCHAR(50),
  PRIMARY KEY(id)
);

INSERT INTO `sort` VALUES 
(1,'abracalanimus','ARC EN CIEL',1,NULL),
(2,'jesus','EAU',12,'MARECAGE'),
(3,'boule de feu','FEU',15,NULL),
(4,'boule de glace','EAU',17,'DESERT');


DROP TABLE IF EXISTS `OBJET`;

CREATE TABLE `OBJET` (
  id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
  nom VARCHAR(50) NOT NULL,
  type_objet VARCHAR(50) NOT NULL,
  sort INTEGER UNSIGNED,
  PRIMARY KEY(id),
  INDEX OBJET_FKIndex1(sort),
  FOREIGN KEY(sort) REFERENCES SORT(id)
);

INSERT INTO `objet` VALUES 
(1,'EXCALIBUR','ARTEFACT',3),
(2,'POMME','NOURRITURE',NULL),
(3,'BROSSE WC','ARTEFACT',1),
(4,'BRIOCHE','NOURRITURE',NULL);


DROP TABLE IF EXISTS `PERSONNAGE`;

CREATE TABLE `PERSONNAGE` (
  id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
  terrain INTEGER UNSIGNED NOT NULL,
  classe INTEGER UNSIGNED NOT NULL,
  joueur VARCHAR(50) NOT NULL,
  nom VARCHAR(50) NOT NULL,
  jauge_magie INTEGER UNSIGNED NOT NULL DEFAULT 0,
  jauge_vie INTEGER UNSIGNED NOT NULL DEFAULT 0,
  sac_capacite INTEGER UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY(id),
  FOREIGN KEY(joueur) REFERENCES JOUEUR(pseudo),
  FOREIGN KEY(classe) REFERENCES CLASSE(id),
  FOREIGN KEY(terrain) REFERENCES TERRAIN(id)
);

INSERT INTO `personnage` VALUES 
(1,1,1,'stifo22','pierre',999,999,999),
(2,3,4,'super_tatasse','superpoulette',500,21,50),
(3,2,5,'georges','pouloulou',1,5001,1);

DROP TABLE IF EXISTS `equipement`;

CREATE TABLE `equipement` (
  `personnage` int unsigned NOT NULL,
  `objet` int unsigned NOT NULL,
  PRIMARY KEY (`personnage`,`objet`),
  KEY `PERSONNAGE_has_OBJET_FKIndex1` (`personnage`),
  KEY `PERSONNAGE_has_OBJET_FKIndex2` (`objet`),
  CONSTRAINT `equipement_ibfk_1` FOREIGN KEY (`personnage`) REFERENCES `personnage` (`id`),
  CONSTRAINT `equipement_ibfk_2` FOREIGN KEY (`objet`) REFERENCES `objet` (`id`)
);

INSERT INTO `equipement` VALUES 
(1,1),
(1,2),
(2,3),
(2,4),
(3,3);

DROP TABLE IF EXISTS `grimoire`;

CREATE TABLE `grimoire` (
  personnage INTEGER UNSIGNED NOT NULL,
  sort INTEGER UNSIGNED NOT NULL,
  PRIMARY KEY(personnage, sort),
  FOREIGN KEY (`personnage`) REFERENCES `personnage` (`id`),
  FOREIGN KEY (`sort`) REFERENCES `sort` (`id`)
);

INSERT INTO `grimoire` VALUES 
(1,1),
(1,2),
(2,3),
(2,4),
(3,3);

-- Quelques requêtes à écrire pour évaluer l’utilisation possible de la base de données :
-- 1. Afficher un joueur avec tous ses personnages
SELECT * FROM personnage WHERE joueur='stifo22';

-- 2. Vérifier si un personnage a de la nourriture avec lui
SELECT o.nom from objet o
JOIN equipement e ON o.id = e.objet
JOIN personnage p ON e.personnage = p.id

WHERE o.type_objet = "NOURRITURE"
and p.nom = "superpoulette";

-- 3. Afficher un personnage avec la liste de sorts qu’il connaît
SELECT s.nom from sort s
JOIN grimoire g ON g.sort = s.id
JOIN personnage p ON g.personnage = p.id

and p.nom = "superpoulette";

-- 4. Afficher un personnage avec la liste de <mark>sorts qu’il connaît qui dépendent d’artefacts</mark>
-- qu’il possède
SELECT s.nom as sort, o.nom as artefact  FROM sort s
JOIN objet o on s.id = o.sort
JOIN equipement e ON o.id = e.objet
JOIN personnage p ON e.personnage = p.id
WHERE p.nom = "superpoulette";

-- 5. Dire quels sorts peut utiliser un personnage en fonction du terrain où il se trouve
SELECT s.nom as sort, o.nom as artefact  FROM sort s
JOIN objet o on s.id = o.sort
JOIN equipement e ON o.id = e.objet
JOIN personnage p ON e.personnage = p.id
JOIN grimoire g ON g.sort = s.id
JOIN personnage p ON g.personnage = p.id

WHERE p.nom = "superpoulette";

SELECT * 
FROM (
SELECT s.nom as sort_artefact  FROM sort s
JOIN objet o on s.id = o.sort
JOIN equipement e ON o.id = e.objet
JOIN personnage p ON e.personnage = p.id
where p.nom = "superpoulette"
) AS t1
CROSS JOIN
(
SELECT s.nom as sort from sort s
JOIN grimoire g ON g.sort = s.id
JOIN personnage p ON g.personnage = p.id
where p.nom = "superpoulette"
) AS t2
;
-- 6. Essayer d’ajouter un objet à un personnage qui est déjà à son <mark>maximum d’objets</mark>
-- emportés


