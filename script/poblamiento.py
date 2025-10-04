import pyodbc
import random
import datetime
from faker import Faker
from tqdm import tqdm
import os
from dotenv import load_dotenv
import sys

# Cargar variables de entorno
load_dotenv()

# Configuración de Faker para datos en español
fake = Faker('es_ES')  # Perú
Faker.seed(42)
random.seed(42)

# Variables de entorno para SQL Server
SERVER = os.getenv('DB_SERVER', 'SISTEMAS-PC')
DATABASE = os.getenv('DB_NAME', 'EssaludDb22')
DRIVER = os.getenv('DB_DRIVER', '{ODBC Driver 17 for SQL Server}')

# Configuración de cantidades de registros
BATCH_SIZE = 10000  # Tamaño del batch para inserciones masivas
RECORDS_PER_TABLE = 1000000  # 1 millón de registros base

# Configuración ajustable de registros
RECORDS_CONFIG = {
    'organizaciones': 100,  # Pocas organizaciones
    'personal': 5000,  # Personal médico limitado
    'pacientes': RECORDS_PER_TABLE,
    'medicamentos': 10000,  # Catálogo de medicamentos
    'ordenes_compra': 50000,
    'detalle_ordenes_compra': 200000,
    'recepciones': 45000,
    'detalle_recepciones': 180000,
    'lotes': 100000,
    'stock': 500000,
    'recetas': RECORDS_PER_TABLE,
    'detalle_recetas': RECORDS_PER_TABLE * 3,  # 3 medicamentos promedio por receta
    'ventas': RECORDS_PER_TABLE,
    'detalle_ventas': RECORDS_PER_TABLE * 2,
    'movimientos_inventario': RECORDS_PER_TABLE * 2,
    'pronosticos_demanda': 120000  # 10000 medicamentos x 12 meses
}

class EsSaludDBPopulator:
    def __init__(self):
        """Inicializar conexión a la base de datos"""
        try:
            # Conexión con autenticación de Windows
            self.conn_string = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
            print(f"Conectando a SQL Server...")
            print(f"  Servidor: {SERVER}")
            print(f"  Base de datos: {DATABASE}")
            print(f"  Autenticación: Windows")
            
            self.conn = pyodbc.connect(self.conn_string)
            self.cursor = self.conn.cursor()
            print("✓ Conexión exitosa a SQL Server\n")
            
            # Deshabilitar restricciones temporalmente para carga masiva
            self.toggle_constraints(False)
            
        except Exception as e:
            print(f"✗ Error al conectar a la base de datos: {e}")
            print("\nAsegúrate de:")
            print("1. SQL Server está ejecutándose")
            print("2. La base de datos 'EssaludDb22' existe")
            print("3. Tienes permisos de Windows para acceder")
            print("4. El driver ODBC está instalado")
            sys.exit(1)
    
    def toggle_constraints(self, enable=True):
        """Habilitar o deshabilitar constraints de foreign keys"""
        action = "CHECK" if enable else "NOCHECK"
        try:
            # Obtener todas las foreign keys
            self.cursor.execute("""
                SELECT 
                    'ALTER TABLE ' + OBJECT_NAME(parent_object_id) + 
                    ' ' + ? + ' CONSTRAINT ' + name
                FROM sys.foreign_keys
            """, action)
            
            constraints = self.cursor.fetchall()
            for constraint in constraints:
                self.cursor.execute(constraint[0])
            
            self.conn.commit()
            print(f"{'✓ Habilitadas' if enable else '✓ Deshabilitadas'} las restricciones de foreign keys")
        except Exception as e:
            print(f"Advertencia al modificar constraints: {e}")
    
    def clear_tables(self):
        """Limpiar todas las tablas en orden correcto"""
        print("Limpiando tablas existentes...")
        tables = [
            'Movimientos_Inventario', 'Detalle_Ventas', 'Ventas',
            'Detalle_Recetas', 'Recetas', 'Stock', 'Lotes',
            'Detalle_Recepciones', 'Recepciones', 'Detalle_Ordenes_Compra',
            'Ordenes_Compra', 'Pronosticos_Demanda', 'Medicamentos',
            'Personal', 'Pacientes', 'Organizaciones'
        ]
        
        for table in tables:
            try:
                # Resetear IDENTITY
                self.cursor.execute(f"DELETE FROM {table}")
                self.cursor.execute(f"DBCC CHECKIDENT ('{table}', RESEED, 0)")
                self.conn.commit()
            except Exception as e:
                print(f"  Advertencia al limpiar {table}: {e}")
        
        print("✓ Tablas limpiadas\n")
    
    def insert_batch(self, table, columns, data):
        """Insertar datos en batch"""
        if not data:
            return
        
        placeholders = ','.join(['?' for _ in columns])
        query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        
        try:
            self.cursor.executemany(query, data)
            self.conn.commit()
        except Exception as e:
            print(f"  Error en batch: {e}")
            # Intentar insertar uno por uno para identificar el problema
            for row in data[:5]:  # Solo mostrar primeros 5 errores
                try:
                    self.cursor.execute(query, row)
                    self.conn.commit()
                except Exception as e2:
                    print(f"    Error en fila: {row[:3]}... - {e2}")
            raise
    
    def populate_organizaciones(self):
        """Poblar tabla Organizaciones"""
        print("Poblando Organizaciones...")
        
        sedes = ['Lima', 'Arequipa', 'Cusco', 'Trujillo', 'Piura', 'Chiclayo', 'Huancayo', 'Iquitos']
        areas = ['Farmacia', 'Emergencia', 'Consultorios', 'Hospitalización', 'UCI', 'Laboratorio', 'Rayos X']
        
        data = []
        for i in range(RECORDS_CONFIG['organizaciones']):
            data.append((
                f"Hospital {fake.company()}",
                random.choice(sedes),
                random.choice(areas)
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Organizaciones', ['nombre', 'sede', 'area'], data)
                data = []
        
        if data:
            self.insert_batch('Organizaciones', ['nombre', 'sede', 'area'], data)
        
        print(f"✓ {RECORDS_CONFIG['organizaciones']} organizaciones creadas\n")
    
    def populate_pacientes(self):
        """Poblar tabla Pacientes"""
        print("Poblando Pacientes...")
        
        generos = ['Masculino', 'Femenino', 'Otro']
        tipos_seguro = ['SIS', 'EsSalud Regular', 'EsSalud Agrario', 'EsSalud Independiente', 'Particular']
        
        data = []
        for i in tqdm(range(RECORDS_CONFIG['pacientes']), desc="  Generando pacientes"):
            genero = random.choices(generos, weights=[45, 45, 10])[0]
            
            if genero == 'Masculino':
                nombre = fake.name_male()
            elif genero == 'Femenino':
                nombre = fake.name_female()
            else:
                nombre = fake.name()
            
            data.append((
                nombre,
                fake.date_of_birth(minimum_age=0, maximum_age=90),
                genero,
                random.choice(tipos_seguro)
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Pacientes', ['nombre', 'fecha_nacimiento', 'genero', 'tipo_seguro'], data)
                data = []
        
        if data:
            self.insert_batch('Pacientes', ['nombre', 'fecha_nacimiento', 'genero', 'tipo_seguro'], data)
        
        print(f"✓ {RECORDS_CONFIG['pacientes']} pacientes creados\n")
    
    def populate_personal(self):
        """Poblar tabla Personal"""
        print("Poblando Personal...")
        
        cargos = ['Médico', 'Enfermera', 'Farmacéutico', 'Técnico', 'Auxiliar', 'Administrativo']
        turnos = ['Mañana', 'Tarde', 'Noche', 'Administrativo']
        generos = ['Masculino', 'Femenino', 'Otro']
        especialidades = ['Cardiología', 'Pediatría', 'Ginecología', 'Traumatología', 'Medicina General',
                         'Neurología', 'Oftalmología', 'Dermatología', None, None]  # Algunos sin especialidad
        estados = ['Activo', 'Activo', 'Activo', 'Activo', 'Inactivo']  # 80% activos
        
        data = []
        for i in tqdm(range(RECORDS_CONFIG['personal']), desc="  Generando personal"):
            genero = random.choice(generos)
            cargo = random.choice(cargos)
            
            if genero == 'Masculino':
                nombre = fake.name_male()
            elif genero == 'Femenino':
                nombre = fake.name_female()
            else:
                nombre = fake.name()
            
            especialidad = random.choice(especialidades) if cargo == 'Médico' else None
            
            data.append((
                nombre,
                cargo,
                random.choice(turnos),
                genero,
                especialidad,
                random.choice(estados)
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Personal', 
                                ['nombre_completo', 'cargo', 'turno', 'genero', 'especialidad', 'estado'], 
                                data)
                data = []
        
        if data:
            self.insert_batch('Personal', 
                            ['nombre_completo', 'cargo', 'turno', 'genero', 'especialidad', 'estado'], 
                            data)
        
        print(f"✓ {RECORDS_CONFIG['personal']} personal creado\n")
    
    def populate_medicamentos(self):
        """Poblar tabla Medicamentos"""
        print("Poblando Medicamentos...")
        
        # Listas de medicamentos reales
        nombres_genericos = [
            'Paracetamol', 'Ibuprofeno', 'Amoxicilina', 'Omeprazol', 'Metformina',
            'Atorvastatina', 'Losartán', 'Aspirina', 'Diclofenaco', 'Cetirizina',
            'Loratadina', 'Ranitidina', 'Ciprofloxacino', 'Azitromicina', 'Prednisona',
            'Salbutamol', 'Insulina', 'Levotiroxina', 'Amlodipino', 'Metoprolol'
        ]
        
        tipos = ['Analgésico', 'Antibiótico', 'Antihipertensivo', 'Antidiabético', 
                'Antiinflamatorio', 'Antihistamínico', 'Vitamina', 'Antiácido']
        
        unidades = ['Tableta', 'Cápsula', 'Ampolla', 'Frasco', 'Sobre', 'Jarabe', 'Crema', 'Ungüento']
        prioridades = ['Alta', 'Media', 'Baja']
        
        data = []
        for i in tqdm(range(RECORDS_CONFIG['medicamentos']), desc="  Generando medicamentos"):
            nombre_generico = f"{random.choice(nombres_genericos)} {random.randint(100, 1000)}mg"
            nombre_comercial = f"{fake.company().split()[0]} {random.randint(100, 500)}"
            
            costo = round(random.uniform(0.5, 100), 2)
            margen = random.uniform(1.2, 2.5)  # Margen de ganancia
            
            data.append((
                nombre_generico,
                nombre_comercial,
                random.choice(tipos),
                random.choice(unidades),
                random.choice(prioridades),
                costo,
                round(costo * margen, 2)
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Medicamentos', 
                                ['nombre_generico', 'nombre_comercial', 'tipo', 'unidad_manejo',
                                 'prioridad', 'costo_unitario_ultima_compra', 'precio_unitario_venta'], 
                                data)
                data = []
        
        if data:
            self.insert_batch('Medicamentos', 
                            ['nombre_generico', 'nombre_comercial', 'tipo', 'unidad_manejo',
                             'prioridad', 'costo_unitario_ultima_compra', 'precio_unitario_venta'], 
                            data)
        
        print(f"✓ {RECORDS_CONFIG['medicamentos']} medicamentos creados\n")
    
    def populate_ordenes_compra(self):
        """Poblar tabla Ordenes_Compra"""
        print("Poblando Órdenes de Compra...")
        
        # Obtener IDs de personal
        self.cursor.execute("SELECT personal_id FROM Personal WHERE cargo IN ('Farmacéutico', 'Administrativo')")
        personal_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not personal_ids:
            print("  ✗ No hay personal disponible")
            return
        
        estados = ['Generada', 'Enviada', 'Parcialmente Recibida', 'Completamente Recibida', 'Cancelada']
        
        data = []
        for i in tqdm(range(RECORDS_CONFIG['ordenes_compra']), desc="  Generando órdenes"):
            fecha_emision = fake.date_between(start_date='-2y', end_date='today')
            fecha_entrega = fecha_emision + datetime.timedelta(days=random.randint(1, 15))
            
            data.append((
                random.choice(personal_ids),
                fecha_emision,
                fecha_entrega,
                round(random.uniform(1000, 50000), 2),
                random.choice(estados)
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Ordenes_Compra', 
                                ['personal_id', 'fecha_emision', 'fecha_entrega_esperada',
                                 'costo_total_estimado', 'estado'], 
                                data)
                data = []
        
        if data:
            self.insert_batch('Ordenes_Compra', 
                            ['personal_id', 'fecha_emision', 'fecha_entrega_esperada',
                             'costo_total_estimado', 'estado'], 
                            data)
        
        print(f"✓ {RECORDS_CONFIG['ordenes_compra']} órdenes creadas\n")
    
    def populate_detalle_ordenes_compra(self):
        """Poblar tabla Detalle_Ordenes_Compra"""
        print("Poblando Detalle de Órdenes de Compra...")
        
        # Obtener IDs
        self.cursor.execute("SELECT orden_id FROM Ordenes_Compra")
        orden_ids = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT medicamento_id, costo_unitario_ultima_compra FROM Medicamentos")
        medicamentos = self.cursor.fetchall()
        
        if not orden_ids or not medicamentos:
            print("  ✗ No hay órdenes o medicamentos disponibles")
            return
        
        data = []
        for i in tqdm(range(RECORDS_CONFIG['detalle_ordenes_compra']), desc="  Generando detalles"):
            medicamento = random.choice(medicamentos)
            cantidad = random.randint(10, 500)
            costo_unitario = medicamento[1] * random.uniform(0.9, 1.1)  # Variación del 10%
            
            data.append((
                random.choice(orden_ids),
                medicamento[0],
                cantidad,
                round(costo_unitario, 2),
                round(cantidad * costo_unitario, 2)
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Detalle_Ordenes_Compra', 
                                ['orden_id', 'medicamento_id', 'cantidad_solicitada',
                                 'costo_unitario_compra', 'subtotal'], 
                                data)
                data = []
        
        if data:
            self.insert_batch('Detalle_Ordenes_Compra', 
                            ['orden_id', 'medicamento_id', 'cantidad_solicitada',
                             'costo_unitario_compra', 'subtotal'], 
                            data)
        
        print(f"✓ {RECORDS_CONFIG['detalle_ordenes_compra']} detalles creados\n")
    
    def populate_recepciones(self):
        """Poblar tabla Recepciones"""
        print("Poblando Recepciones...")
        
        # Obtener órdenes que pueden tener recepciones
        self.cursor.execute("""
            SELECT orden_id FROM Ordenes_Compra 
            WHERE estado IN ('Parcialmente Recibida', 'Completamente Recibida')
        """)
        orden_ids = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT personal_id FROM Personal WHERE cargo = 'Farmacéutico'")
        personal_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not orden_ids or not personal_ids:
            print("  ✗ No hay órdenes o personal disponible")
            return
        
        data = []
        for i in tqdm(range(min(RECORDS_CONFIG['recepciones'], len(orden_ids))), desc="  Generando recepciones"):
            data.append((
                orden_ids[i % len(orden_ids)],
                random.choice(personal_ids),
                fake.date_time_between(start_date='-1y', end_date='now'),
                f"GR-{random.randint(1000, 9999)}-{random.randint(100000, 999999)}",
                fake.text(max_nb_chars=200) if random.random() > 0.7 else None
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Recepciones', 
                                ['orden_id', 'personal_id', 'fecha_recepcion',
                                 'guia_remision_proveedor', 'observaciones'], 
                                data)
                data = []
        
        if data:
            self.insert_batch('Recepciones', 
                            ['orden_id', 'personal_id', 'fecha_recepcion',
                             'guia_remision_proveedor', 'observaciones'], 
                            data)
        
        print(f"✓ {min(RECORDS_CONFIG['recepciones'], len(orden_ids))} recepciones creadas\n")
    
    def populate_detalle_recepciones(self):
        """Poblar tabla Detalle_Recepciones"""
        print("Poblando Detalle de Recepciones...")
        
        # Obtener datos necesarios
        self.cursor.execute("""
            SELECT r.recepcion_id, d.detalle_orden_id, d.medicamento_id, d.cantidad_solicitada
            FROM Recepciones r
            INNER JOIN Detalle_Ordenes_Compra d ON r.orden_id = d.orden_id
        """)
        recepciones_detalles = self.cursor.fetchall()
        
        if not recepciones_detalles:
            print("  ✗ No hay recepciones con detalles disponibles")
            return
        
        data = []
        for i in tqdm(range(min(RECORDS_CONFIG['detalle_recepciones'], len(recepciones_detalles))), 
                     desc="  Generando detalles"):
            detalle = random.choice(recepciones_detalles)
            cantidad_recibida = random.randint(int(detalle[3] * 0.8), detalle[3])  # 80-100% de lo solicitado
            
            data.append((
                detalle[0],  # recepcion_id
                detalle[1],  # detalle_orden_id
                detalle[2],  # medicamento_id
                cantidad_recibida,
                f"LOTE-{random.randint(100000, 999999)}",
                fake.date_between(start_date='today', end_date='+2y')
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Detalle_Recepciones', 
                                ['recepcion_id', 'detalle_orden_id', 'medicamento_id',
                                 'cantidad_recibida', 'codigo_lote', 'fecha_vencimiento'], 
                                data)
                data = []
        
        if data:
            self.insert_batch('Detalle_Recepciones', 
                            ['recepcion_id', 'detalle_orden_id', 'medicamento_id',
                             'cantidad_recibida', 'codigo_lote', 'fecha_vencimiento'], 
                            data)
        
        print(f"✓ {min(RECORDS_CONFIG['detalle_recepciones'], len(recepciones_detalles))} detalles creados\n")
    
    def populate_lotes(self):
        """Poblar tabla Lotes"""
        print("Poblando Lotes...")
        
        # Obtener detalles de recepciones
        self.cursor.execute("""
            SELECT detalle_recepcion_id, medicamento_id, codigo_lote, 
                   fecha_vencimiento, cantidad_recibida
            FROM Detalle_Recepciones
        """)
        detalles = self.cursor.fetchall()
        
        if not detalles:
            print("  ✗ No hay detalles de recepciones disponibles")
            return
        
        estados = ['Disponible', 'Disponible', 'Disponible', 'En Cuarentena', 'Vencido', 'Agotado']
        
        data = []
        for i in tqdm(range(min(RECORDS_CONFIG['lotes'], len(detalles))), desc="  Generando lotes"):
            detalle = detalles[i % len(detalles)]
            
            data.append((
                detalle[1],  # medicamento_id
                detalle[0],  # detalle_recepcion_id
                detalle[2],  # codigo_lote
                detalle[3],  # fecha_vencimiento
                fake.date_time_between(start_date='-1y', end_date='now'),
                detalle[4],  # cantidad_inicial
                random.choice(estados)
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Lotes', 
                                ['medicamento_id', 'detalle_recepcion_id', 'codigo_lote',
                                 'fecha_vencimiento', 'fecha_ingreso', 'cantidad_inicial', 'estado'], 
                                data)
                data = []
        
        if data:
            self.insert_batch('Lotes', 
                            ['medicamento_id', 'detalle_recepcion_id', 'codigo_lote',
                             'fecha_vencimiento', 'fecha_ingreso', 'cantidad_inicial', 'estado'], 
                            data)
        
        print(f"✓ {min(RECORDS_CONFIG['lotes'], len(detalles))} lotes creados\n")
    
    def populate_stock(self):
        """Poblar tabla Stock"""
        print("Poblando Stock...")
        
        # Obtener IDs
        self.cursor.execute("SELECT lote_id, cantidad_inicial FROM Lotes WHERE estado = 'Disponible'")
        lotes = self.cursor.fetchall()
        
        self.cursor.execute("SELECT organizacion_id FROM Organizaciones")
        organizacion_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not lotes or not organizacion_ids:
            print("  ✗ No hay lotes u organizaciones disponibles")
            return
        
        data = []
        used_combinations = set()
        
        for i in tqdm(range(RECORDS_CONFIG['stock']), desc="  Generando stock"):
            lote = random.choice(lotes)
            org_id = random.choice(organizacion_ids)
            
            # Evitar duplicados de lote_id + organizacion_id
            combination = (lote[0], org_id)
            if combination in used_combinations:
                continue
            
            used_combinations.add(combination)
            cantidad_actual = random.randint(0, lote[1])  # Entre 0 y cantidad inicial
            
            data.append((
                lote[0],
                org_id,
                cantidad_actual
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Stock', ['lote_id', 'organizacion_id', 'cantidad_actual'], data)
                data = []
        
        if data:
            self.insert_batch('Stock', ['lote_id', 'organizacion_id', 'cantidad_actual'], data)
        
        print(f"✓ {len(used_combinations)} registros de stock creados\n")
    
    def populate_recetas(self):
        """Poblar tabla Recetas"""
        print("Poblando Recetas...")
        
        # Obtener IDs
        self.cursor.execute("SELECT paciente_id FROM Pacientes")
        paciente_ids = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT personal_id FROM Personal WHERE cargo = 'Médico'")
        medico_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not paciente_ids or not medico_ids:
            print("  ✗ No hay pacientes o médicos disponibles")
            return
        
        diagnosticos = [
            'Hipertensión arterial', 'Diabetes mellitus tipo 2', 'Infección respiratoria aguda',
            'Gastritis aguda', 'Cefalea tensional', 'Lumbalgia', 'Ansiedad generalizada',
            'Dermatitis atópica', 'Conjuntivitis viral', 'Faringitis aguda'
        ]
        
        estados = ['Pendiente', 'Parcialmente Surtida', 'Completamente Surtida', 'Vencida']
        
        data = []
        for i in tqdm(range(RECORDS_CONFIG['recetas']), desc="  Generando recetas"):
            data.append((
                random.choice(paciente_ids),
                random.choice(medico_ids),
                fake.date_between(start_date='-1y', end_date='today'),
                random.choice(diagnosticos),
                random.choice(estados)
            ))
            
            if len(data) >= BATCH_SIZE:
                self.insert_batch('Recetas', 
                                ['paciente_id', 'personal_id', 'fecha_emision', 'diagnostico', 'estado'], 
                                data)
                data = []
        
        if data:
            self.insert_batch('Recetas', 
                            ['paciente_id', 'personal_id', 'fecha_emision', 'diagnostico', 'estado'], 
                            data)
        
        print(f"✓ {RECORDS_CONFIG['recetas']} recetas creadas\n")
    
    def populate_detalle_recetas(self):
        """Poblar tabla Detalle_Recetas"""
        print("Poblando Detalle de Recetas...")
        
        # Obtener IDs
        self.cursor.execute("SELECT receta_id FROM Recetas")
        receta_ids = [row[0] for row in self.cursor.fetchall()]
        
        self.cursor.execute("SELECT medicamento_id FROM Medicamentos")
        medicamento_ids = [row[0] for row in self.cursor.fetchall()]
        
        if not receta_ids or not medicamento_ids:
            print("  ✗ No hay recetas o medicamentos disponibles")
            return
        
        indicaciones = [
            'Tomar 1 cada 8 horas por 7 días',
            'Tomar 1 cada 12 horas con alimentos',
            'Tomar 1 al día en ayunas',
            'Tomar 2 cada 6 horas por 5 días',
            'Aplicar en la zona afectada 3 veces al día',
            'Tomar 1 antes de dormir',
            'Tomar según necesidad, máximo 3 al día'
        ]
        
        data = []
        for i in tqdm(range(RECORDS_CONFIG['detalle_recetas']), desc="  Generando detalles"):
            data.append(
                random.choice(receta_ids),
                random.choice(medicamento_ids),
                random.randint(1, 30),
                random.choice(indicaciones))
            
if __name__ == "__main__":
    aea = EsSaludDBPopulator()