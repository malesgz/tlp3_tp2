import sys
import MySQLdb

try:
    db = MySQLdb.connect("localhost","root","","db_tp2" )
    print("Conexión a la base de datos exitosa.")
except MySQLdb.Error as e:
    print("No se pudo conectar a la base de datos:",e)
    sys.exit(1)

cursor = db.close()