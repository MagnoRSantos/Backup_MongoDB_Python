# -*- coding: utf-8 -*-

###################################################################################
# Magno Santos - Criacao
#
# Requerimentos:
### python 3.10  64 bits
### modulos io, os, datetime, csv, pyodbc, time, fnmatch, socket, pymongo
###################################################################################

import io
import sys
import os
import time
import calendar
import socket
import logging
from datetime import date, timedelta, datetime
from pymongo import MongoClient
from azure.storage.blob import BlobServiceClient, generate_container_sas, ResourceTypes, ContainerSasPermissions
from bson.timestamp import Timestamp
import dotenv

## Carrega os valores do .env
dotenv.load_dotenv()

TIMESTAMP = datetime.now().strftime('%Y-%m-%d-%H%M')

### User geral
# database host name
MONGO_HOST = os.getenv("MONGO_HOST")
# database port
MONGO_PORT = os.getenv("MONGO_PORT")
# database user name
DBUSERNAME = os.getenv("DBUSERNAME")
# database password
DBPASSWORD = os.getenv("DBPASSWORD")
# authentication database name
DBAUTHDB   = os.getenv("DBAUTHDB")

# ambiente
ENV=socket.gethostname()
# backup diretorio disco local  
BKP_DIR = os.path.join("/backup", ENV)
# backup name
BKP_NAME= f"{ENV}-{TIMESTAMP}"

# variavel global list dbs mongodb
listdbs = []

# variaveis globais do storage account
str_account_name   = os.getenv("str_account_name")
str_account_key    = os.getenv("str_account_key")
str_container_name = os.getenv("str_container_name")

# variavel global do local do app python
datahoraLog = datetime.now().strftime('%Y-%m-%d')
dirapp = os.path.dirname(os.path.realpath(__file__))

dirlogfile = os.path.join(dirapp, "log")
logfile = os.path.join(dirlogfile, f"log_backup_{datahoraLog}.txt")

dirqueryfile = os.path.join(dirapp, "query")
queryfile = os.path.join(dirqueryfile, "query.js")

dirsastoken = os.path.join(dirapp, "sastoken")
sastokenfile = os.path.join(dirsastoken, "sastoken.txt")

##cria os diretórios se não existirem
if not os.path.exists(dirlogfile):
    os.makedirs(dirlogfile)
        
if not os.path.exists(dirqueryfile):
    os.makedirs(dirqueryfile)

if not os.path.exists(dirsastoken):
    os.makedirs(dirsastoken)

## trecho de geração do log
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=logfile, filemode='a'
)

logger = logging.getLogger(__name__)


def geraSasToken(v_account_name, v_account_key, v_container_name):
    sas_token = generate_container_sas(
    account_name=str(v_account_name),
    account_key=str(v_account_key),
    container_name=str(v_container_name),
    resource_types=ResourceTypes(service=True),
    permission=ContainerSasPermissions(read=True, write=True, delete=True, list=True, add=True,create=True),
    start = datetime.now().strftime('%Y-%m-%dT00:00:00Z'),
    expiry=(datetime.now() + timedelta(days=3)).strftime('%Y-%m-%dT00:00:00Z')
    #expiry=datetime.utcnow() + timedelta(hours=1)
    )

    return sas_token

## grava o sastoken em arquivo txt
def gravaSasToken():
    sas = str(geraSasToken(str_account_name, str_account_key, str_container_name))
    with io.open(sastokenfile, 'w', encoding='utf-8') as f:
        f.write(str(sas))


## lê o sastoken do arquivo txt
def lerSasToken():
    with io.open(sastokenfile, 'r', encoding='utf-8') as f:
        v_sastoken = f.read()

    return v_sastoken


## obtem timestamp   
def getTimeStamp():
    try:
        
        connstr = f"mongodb://{DBUSERNAME}:{DBPASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{DBAUTHDB}"

        with MongoClient(connstr) as client:
            v_timestamp = client['local'].oplog.rs.find().sort([("$natural", -1)]).limit(1).next()
            v_timestampts = str(v_timestamp['ts'])
        
        ### cria/escreve arquivo de script temporario javascript
        logging.info(f"Obtendo timestamp: [{v_timestampts}]")
        v_timestampts = v_timestampts.replace('Timestamp', '')
        v_timestampts = v_timestampts.replace('(', '')
        v_timestampts = v_timestampts.replace(')', '')
        v_timestampts = v_timestampts.replace(' ', '')
        array_timestampts = v_timestampts.split(',')
        t = array_timestampts[0]
        i = array_timestampts[1]
        str_query = '{"ts":{"$gt":{"$timestamp":{"t": ' + str(t) + ' , "i": ' + i + ' }}}}'
        print(f'new: {str_query}')
        with io.open(queryfile, 'w', encoding='utf-8') as f:
            f.write(str(str_query))

    except Exception as e:
        print("Error: %s" % e)
        logging.error("Error: %s" % e)
        
        
## funcao de remocao do backup local após envio ao storage
def removeBackupLocal():
    cmdDelete = f"sudo rm -f -r {BKP_DIR}/*"
    print(f"Apagando backup local: {cmdDelete}")
    logging.info(f"Apagando backup local: {cmdDelete}")
    os.popen(cmdDelete)


## funcao de remocao do log do azcopy
def removeLogAzcopy():
    cmdDelete1 = f"sudo rm -rf /home/user/.azcopy/*.log"
    cmdDelete2 = f"sudo rm -rf /root/.azcopy/*.log"
    print(f"Apagando logs do azcopy: {cmdDelete1}\n{cmdDelete2}")
    logging.info(f"Apagando logs do azcopy: {cmdDelete1}")
    logging.info(f"Apagando logs do azcopy: {cmdDelete2}")
    os.popen(cmdDelete1)
    os.popen(cmdDelete2)


# Lista databases no mongodb
def databaseMongodb():
    try:
     
        connstr = f"mongodb://{DBUSERNAME}:{DBPASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{DBAUTHDB}"

        with MongoClient(connstr) as client:
            cursor = client.list_database_names() #listar todos databases

        for dbcursor in cursor:
            listdbs.append(dbcursor)
    
        return listdbs

    except Exception as e:
        print("Error: %s" % e)
        logging.error("Error: %s" % e)

## funcao de backup de todos os databases
def BackupAllDbs():

    #sas = str(geraSasToken(str_account_name, str_account_key, str_container_name))
    gravaSasToken()
    sas = str(lerSasToken())
    v_localbkp = os.path.join(BKP_DIR, BKP_NAME)
    v_localbkp = f"{v_localbkp}_FULL"
    cmdMongodump = f"sudo mongodump -u {DBUSERNAME} -p {DBPASSWORD} --host {MONGO_HOST} --port {MONGO_PORT} --numParallelCollections 8 --authenticationDatabase {DBAUTHDB} --readPreference=secondary --oplog --out {v_localbkp} --gzip"
    str_cmdMongodump = f"sudo mongodump -u {DBUSERNAME} -p ******** --host {MONGO_HOST} --port {MONGO_PORT} --numParallelCollections 8 --authenticationDatabase {DBAUTHDB} --readPreference=secondary --oplog --out {v_localbkp} --gzip"
    
    print(f"Executando MongoDump...")
    logging.info(f"MongoDump - Início")
    print(str_cmdMongodump)
    logging.info(str_cmdMongodump)
    os.popen(cmdMongodump).read()
    getTimeStamp()
    logging.info("MongoDump - Fim")

    ## verifica existencia da pasta e chama o azcopy
    pathLocalAux = f"{v_localbkp}/"
    
    if os.path.isdir(pathLocalAux):
        msg = f"Diretório [{v_localbkp}] a ser enviado para storage. Executando azcopy."
        print(msg)
        logging.info(msg)
        logging.info("Envio de dados pelo azcopy - Início.")
        ExecutaAzcopy(v_localbkp, sas)
        logging.info("Envio de dados pelo azcopy - Fim.")
        removeBackupLocal()
    else:
        msg = f"Diretório {v_localbkp} não existe..."
        logging.info(msg)
        print(msg)

## funcao de backup de databases especificos
def BackupEspecificDbs(p_listdbs):

    #sas = str(geraSasToken(str_account_name, str_account_key, str_container_name))
    gravaSasToken()
    sas = str(lerSasToken())
    dbsmongo = databaseMongodb()
    #print(dbsmongo[1:-1])
    #print(p_listdbs)
    

    listOfstr = list(p_listdbs.split(','))
    for dbs in listOfstr:
        if dbs in dbsmongo:
            
            print(f"Database: {dbs} existente para Backup.")
            logging.info(f"Database: {dbs} existente para Backup.")
            v_localbkpdb = "backup-" + dbs
            v_localbkp = os.path.join(os.path.join(BKP_DIR, BKP_NAME), v_localbkpdb)

            cmdMongodump = f"sudo mongodump -u {DBUSERNAME} -p {DBPASSWORD} --host {MONGO_HOST} --port {MONGO_PORT} --numParallelCollections 8 --authenticationDatabase {DBAUTHDB} --readPreference=secondary --db={dbs} --out {v_localbkp} --gzip"
            str_cmdMongodump = f"sudo mongodump -u {DBUSERNAME} -p ******** --host {MONGO_HOST} --port {MONGO_PORT} --numParallelCollections 8 --authenticationDatabase {DBAUTHDB} --readPreference=secondary --db={dbs} --out {v_localbkp} --gzip"
            
            print("Executando MongoDump...")
            logging.info(f"MongoDump {dbs} - Início")
            print(str_cmdMongodump)
            logging.info(str_cmdMongodump)
            os.popen(cmdMongodump).read()
            logging.info(f"MongoDump {dbs} - Fim")
            print("\n===========================================================================\n")
        
        else:
            print(f"Database: {dbs} não existente para Backup.")
            print("\n===========================================================================\n")
            logging.info(f"Database: {dbs} não existente para Backup.")

    ## verifica existencia da pasta e chama o azcopy
    pathLocal = os.path.join(BKP_DIR, BKP_NAME) 
    pathLocalAux = f"{pathLocal}/"
    
    if os.path.isdir(pathLocalAux):
        msg = f"Diretório [{pathLocal}] a ser enviado para storage. Executando azcopy."
        print(msg)
        logging.info(f"Diretório [{pathLocal}] a ser enviado para storage.")
        logging.info("Envio de dados pelo azcopy - Início.")
        ExecutaAzcopy(pathLocal, sas)
        logging.info("Envio de dados pelo azcopy - Fim.")
        removeBackupLocal()
    else:
        msg = f"Diretório {pathLocal} não existe..."
        print(msg)
        logging.info(msg)


## funcao de backup somente do OpLog
def BackupOnlyOpLog():

    sas = str(geraSasToken(str_account_name, str_account_key, str_container_name))
    v_localbkp = os.path.join(BKP_DIR, BKP_NAME)
    v_localbkp = v_localbkp + "_OPLOG"
    cmdMongodump = f"sudo mongodump -u {DBUSERNAME} -p {DBPASSWORD} --host {MONGO_HOST} --port {MONGO_PORT} --authenticationDatabase {DBAUTHDB} -d local -c oplog.rs --queryFile {queryfile} --out {v_localbkp} --gzip"
    str_cmdMongodump = f"sudo mongodump -u {DBUSERNAME} -p ******** --host {MONGO_HOST} --port {MONGO_PORT} --authenticationDatabase {DBAUTHDB} -d local -c oplog.rs --queryFile {queryfile} --out {v_localbkp} --gzip"

    print("Executando MongoDump...")
    logging.info("MongoDump - Início")
    print(str_cmdMongodump)
    logging.info(str_cmdMongodump)
    os.popen(cmdMongodump).read()
    getTimeStamp()
    logging.info("MongoDump - Fim")
    
    ## verifica existencia da pasta e chama o azcop
    pathLocalAux = f"{v_localbkp}/"

    if os.path.isdir(pathLocalAux):
        msg = f"Diretório [{v_localbkp}] a ser enviado para storage. Executando azcopy."
        print(msg)
        logging.info(msg)
        logging.info("Envio de dados pelo azcopy - Início.")
        ExecutaAzcopy(v_localbkp, sas)
        logging.info("Envio de dados pelo azcopy - Fim.")
        removeBackupLocal()
    else:
        msg = f"Diretório {v_localbkp} não existe..."
        print(msg)
        logging.info(msg)

## funcao que identifica o tipo de backup a ser realizado
def TypeBackup(v_typeBackp):
    msgbkp = "Selecionado o tipo de backup: "

    # All - todos os dbs (default)
    # OpLog - backup somente do oplog

    # All - todos os dbs (default)
    if(v_typeBackp == "all"):
        print(f"{msgbkp} {v_typeBackp}")
        print("Escolhido backup de todos os dbs (default)\n")
        logging.info(f"{msgbkp} {v_typeBackp}")
        logging.info("Escolhido backup de todos os dbs (default)")
        BackupAllDbs()

    # OpLog - backup somente do oplog
    elif(v_typeBackp == "oplog"):
        print(f"{msgbkp} {v_typeBackp}")
        print("Escolhido backup somente do Oplog\n")
        logging.info(f"{msgbkp} {v_typeBackp}")
        logging.info("Escolhido backup somente do Oplog")
        BackupOnlyOpLog()

    # backup de Db especifico
    else:
        v_listdbs = v_typeBackp
        v_typeBackp = "Backups de databases especificos"
        print(f"{msgbkp} {v_typeBackp}")
        print(f"Escolhido backup de databases especificos: {v_listdbs}\n")
        logging.info(f"{msgbkp} {v_typeBackp}")
        logging.info(f"Escolhido backup de databases especificos: {v_listdbs}")
        BackupEspecificDbs(v_listdbs)


def ExecutaAzcopy(v_localbkp, sastoken):
    
    # variaveis de origem e destino para  envio pelo azcopy
    SOURCE = v_localbkp
    TARGET = f"https://{str_account_name}.blob.core.windows.net/{str_container_name}/{ENV}?{sastoken}"

    cmdAzcopy = f'/usr/bin/azcopy copy "{SOURCE}" "{TARGET}" --recursive=true'
    print(cmdAzcopy)
    
    logCmdBkp = os.popen(cmdAzcopy).read()
    print(logCmdBkp)
    logging.info(logCmdBkp)

def main():
    
    ## log inicio
    logging.info(f"*****Início Backup MongoDB*****")
    
    ## chamada de limpeza dos logs do azcopy
    removeLogAzcopy()
    
    ## recebe o tipo de backup a ser feito
    arg_typeBackp = str(sys.argv[1])
    
    ### testes
    #arg_typeBackp = "db_000XX" #"db_98000, db_97998, db_000XX"
    #arg_typeBackp = "OPLOG"
    #arg_typeBackp = "ALL"

    arg_typeBackp = (arg_typeBackp.lower()).replace(' ', '')
    TypeBackup(arg_typeBackp)
    
    ## log fim
    logging.info(f"*****Final Backup MongoDB*****\n")

### INICIO DA APLICACAO
if __name__ == "__main__":
    main()
