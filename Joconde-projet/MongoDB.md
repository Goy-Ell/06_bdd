# -------Install-----------------
## DL :
1. https://www.mongodb.com/try/download/community
1. https://www.mongodb.com/try/download/database-tools
1. https://www.mongodb.com/try/download/shell?jmp=docs

install mongodb
depacer mongosh dans le dossier mongo
deplacer le contenue de la database-tools dans le dossier bin de mongodb

## Param:
ajouter les Path des dossiers bin dans les variables d'environnement

## la la racine de C:/ creer le dossier data/db en cmd ou powershell ou bash :
cmd : `mkdir /data`
cmd : `mkdir /data/db`


# -----Connection--------------------

- CMD

## lancer le daemon de mongo.  
cmd : `mongod`
recup le port d'ecoute a la fin du lancement de mongod dans le cmd (ex : `27010`)

## Importer des données json. ( ca creera la db et coll s ils n'esistent pas)
cmd : `mongoimport --db {database_name} --collection{collection_name} --file {file_path}`

## lancer le shell mongo et se connecter au serveur sur le port 27017
cmd : `mongosh`
For mongosh info see: https://docs.mongodb.com/mongodb-shell/

## quelque ligne mongosh pour se balader sur le serveur:
`show dbs` : db list
`use {db_name}` enter db
`show collections` list of collection in db
`db.{collection_name}.count()` nb line
`db.{collection_name}.fin()` affiche les données
`db.{collection_name}.find( ObjectId('62f35961732e0304531dc9c2'))` for one line
`...findOne()`
`...find({{clé}:{valeur}}`
`...find({{clé}:{valeur},{clé}:{valeur}})`
`...find({{clé}:{valeur},{clé}:{valeur}} , {{clé}:1})`
`...find({{clé}:{valeur},{clé}:{valeur}} , {{clé1}:1,{clé2}:1})`

Un deuxième paramètre (optionnel) de la fonction find permet de choisir les clés à retourner dans le résultat. Pour cela, il faut mettre la clé, et la valeur `1` pour projeter la valeur (on reste dans le concept `clé/valeur`).

...find({`grades` : {$elemMatch : {`grade` : `C`,`score` : {$lt :40}}},  {`grades.grade` : 1,`grades.score` : 1});

`$eleMatch` pour que verifier 2 condition sur le meme element

`db.{collection_name}.distinct({clé})` #equivelent de .unique()

## requete et projection:
https://openclassrooms.com/fr/courses/4462426-maitrisez-les-bases-de-donnees-nosql/4474606-interrogez-vos-donnees-avec-mongodb
https://www.mongodb.com/docs/v3.2/reference/operator/query/

For mongosh info see: https://docs.mongodb.com/mongodb-shell/

# Studio 3T

Permet de faire les meme chose mais via une interface