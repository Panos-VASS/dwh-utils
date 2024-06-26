import pyodbc

# Reemplaza 'YourDSN' con el nombre de tu DSN configurado
connection_string = (
    'DSN=YourDSN;'
    'UID=your_username;'
    'PWD=your_password'
)

try:
    # Establecer la conexión
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    # Ejecutar una consulta de prueba
    cursor.execute('SELECT * FROM table_name')
    
    # Recuperar y mostrar los resultados
    for row in cursor:
        print(row)
    
    # Cerrar el cursor y la conexión
    cursor.close()
    conn.close()
    print("Conexión exitosa y cerrada correctamente.")

except pyodbc.Error as e:
    print("Error en la conexión:", e)