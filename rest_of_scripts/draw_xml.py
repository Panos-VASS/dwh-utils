#%%
import csv
import xml.etree.ElementTree as ET

def create_diagram(tables):
    mxfile = ET.Element('mxfile', {'host': 'app.diagrams.net'})
    diagram = ET.SubElement(mxfile, 'diagram', {'name': 'ER Diagram'})
    mxGraphModel = ET.SubElement(diagram, 'mxGraphModel', {
        'dx': '1170', 'dy': '825', 'grid': '1', 'gridSize': '10', 'guides': '1', 'tooltips': '1', 'connect': '1', 'arrows': '1', 'fold': '1', 'page': '1', 'pageScale': '1', 'pageWidth': '827', 'pageHeight': '1169', 'math': '0', 'shadow': '0'
    })
    root = ET.SubElement(mxGraphModel, 'root')
    ET.SubElement(root, 'mxCell', {'id': '0'})
    ET.SubElement(root, 'mxCell', {'id': '1', 'parent': '0'})
    
    table_x = 20
    table_y = 20
    max_height = 0

    for table_id, (table_name, columns) in enumerate(tables.items(), start=2):
        table_height = 40 + len(columns) * 20
        max_height = max(max_height, table_height)
        
        table_cell = ET.SubElement(root, 'mxCell', {
            'id': str(table_id),
            'value': table_name,
            'style': 'swimlane',
            'vertex': '1',
            'parent': '1'
        })
        ET.SubElement(table_cell, 'mxGeometry', {'x': str(table_x), 'y': str(table_y), 'width': '200', 'height': str(table_height), 'as': 'geometry'})
        
        column_y = 20
        for column_id, column in enumerate(columns, start=1):
            if column['COLUMN_KEY'] == 'PRI':
                column_value = f"Key : {column['COLUMN_NAME']} : {column['DATA_TYPE']} ({column['CHARACTER_MAXIMUM_LENGTH']})"
            else:
                column_value = f" : {column['COLUMN_NAME']} : {column['DATA_TYPE']} ({column['CHARACTER_MAXIMUM_LENGTH']})"
            
            column_cell = ET.SubElement(root, 'mxCell', {
                'id': str(table_id * 100 + column_id),
                'value': column_value,
                'style': 'text;html=1;strokeColor=none;fillColor=none;align=left;verticalAlign=middle;whiteSpace=wrap;rounded=0;overflow=hidden;rotatable=0;fontSize=12;spacingLeft=3;spacingRight=3;',
                'vertex': '1',
                'parent': str(table_id)
            })
            ET.SubElement(column_cell, 'mxGeometry', {'x': '20', 'y': str(column_y), 'width': '160', 'height': '20', 'as': 'geometry'})
            column_y += 20

        table_x += 220  # Move the next table to the right
        if table_x > 1000:  # If tables go off the page, start a new row
            table_x = 20
            table_y += max_height + 40  # Move down by the height of the tallest table plus some padding
            max_height = 0  # Reset max height for the next row
    
    return ET.tostring(mxfile, encoding='utf-8').decode('utf-8')

# Read the CSV file and organize data
tables = {}
with open('tables needed/tablas.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        table_name = row['TABLE_NAME']
        column_name = row['COLUMN_NAME']
        data_type = row['DATA_TYPE']
        column_key = row['COLUMN_KEY']
        character_length = row['CHARACTER_MAXIMUM_LENGTH']
        if table_name not in tables:
            tables[table_name] = []
        tables[table_name].append({
            'COLUMN_NAME': column_name,
            'DATA_TYPE': data_type,
            'COLUMN_KEY': column_key,
            'CHARACTER_MAXIMUM_LENGTH': character_length
        })

# Create the XML content for draw.io
xml_content = create_diagram(tables)
with open('diagram.xml', 'w') as file:
    file.write(xml_content)

# %%
