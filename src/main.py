import sys
import os
import MySQLdb
import csv

# Lee el archivo CSV y devuelve una lista de filas.
def read_csv():
    try:
        with open('src/localidades.csv', newline='') as archivo_csv:
            lector_csv = csv.reader(archivo_csv, delimiter=',', quotechar='"')
            return list(lector_csv)
    except FileNotFoundError as e:
        print(f'No se encontró el archivo. Error: {e}')
        return []

# Conexión a la base de datos.
try:
    db = MySQLdb.connect("localhost", "root", "", "db_tp2")
    print("Conexión a la base de datos exitosa.")
except MySQLdb.Error as e:
    print("No se pudo conectar a la base de datos:", e)
    sys.exit(1)

# Creación de la tabla de base de datos e inserción de datos.
try:
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS localidades")
    cursor.execute("CREATE TABLE IF NOT EXISTS localidades ( provincia VARCHAR(200), id INT, localidad VARCHAR(255), cp VARCHAR(255), id_prov_mstr VARCHAR(200))")
    for row in read_csv(): 
        cursor.execute("INSERT INTO localidades ( provincia, id, localidad, cp, id_prov_mstr) VALUES (%s, %s, %s, %s, %s)", row)
    db.commit()
    print("Datos insertados correctamente.")
except MySQLdb.Error as e:
    print("Error al crear la tabla o insertar datos:", e)
    sys.exit(1)

# Agrupación y exportación de datos.
try:
    if not os.path.exists('Provincias_csv'):
        os.makedirs('Provincias_csv')
    else:
        os.makedirs('Provincias_csv')
except OSError as e:
    print('No se pudo crear la carpeta. Error: %s' % e)
    sys.exit(1)

try:
    cursor = db.cursor()
    cursor.execute("SELECT DISTINCT provincia FROM localidades")
    provincias = cursor.fetchall()
    for provincia in provincias:
        cursor.execute("SELECT * FROM localidades WHERE provincia = %s", (provincia[0],))
        rows = cursor.fetchall()
        with open(f'Provincias_csv/{provincia[0]}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
    print('Agrupación y exportación exitosa.')
except MySQLdb.Error as e:
    print('Error al al agrupar y exportar. Error: %s' % e)
    sys.exit(1)
finally:
    db.close()

try:
    db = MySQLdb.connect("localhost", "root", "", "db_tp2")
    cursor = db.cursor()
    cursor.execute("SELECT DISTINCT provincia FROM localidades")
    provincias = cursor.fetchall()
    for provincia in provincias:
        cursor.execute("SELECT COUNT(*) FROM localidades WHERE provincia = %s", (provincia[0],))
        total_rows = cursor.fetchone()
        total_rows = total_rows[0]
        with open(f'Provincias_csv/{provincia[0]}.csv', 'r') as f:
            reader = csv.reader(f)
            data = list(reader)
except MySQLdb.Error as e:
    print("Error al ejecutar la consulta: ", e)
finally:
    db.close()
