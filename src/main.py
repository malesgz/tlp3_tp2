import sys
import MySQLdb
import csv

# Lee el archivo CSV y devuelve una lista de filas.
def read_csv():
    try:
        with open('src/localidades.csv', newline='') as archivo_csv:
            lector_csv = csv.reader(archivo_csv, delimiter=',', quotechar='"')
            return list(lector_csv)
    except FileNotFoundError as e:
        print(f'No se pudo abrir el archivo. Error: {e}')
        return []

try:
    db = MySQLdb.connect("localhost", "root", "", "db_tp2")
    print("Conexión a la base de datos exitosa.")
except MySQLdb.Error as e:
    print("No se pudo conectar a la base de datos:", e)
    sys.exit(1)

# Creación de la tabla de base de datos e inserción de datos.
try:
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS provincias")
    cursor.execute("CREATE TABLE IF NOT EXISTS provincias (id INT, provincia VARCHAR(200), localidad VARCHAR(255), cp VARCHAR(255), id_prov_mstr VARCHAR(200))")
    for row in read_csv(): 
        cursor.execute("INSERT INTO provincias (id, provincia, localidad, cp, id_prov_mstr) VALUES (%s, %s, %s, %s, %s)", row)
    db.commit()
    print("Datos insertados correctamente.")
except MySQLdb.Error as e:
    print("Error al crear la tabla o insertar datos:", e)
    db.rollback()
    sys.exit(1)

db.close()
